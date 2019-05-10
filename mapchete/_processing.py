from collections import namedtuple
from functools import partial
from itertools import chain
import logging
import multiprocessing
import os

from mapchete.errors import MapcheteNodataTile
from mapchete._timer import Timer

logger = logging.getLogger(__name__)


ProcessInfo = namedtuple('ProcessInfo', 'tile processed process_msg written write_msg')


#############################################################
# wrappers helping to abstract multiprocessing and billiard #
#############################################################

class Executor():
    """
    Wrapper class to be used with multiprocessing or billiard.
    """
    def __init__(
        self,
        start_method="fork",
        max_workers=None,
        multiprocessing_module=multiprocessing
    ):
        self.start_method = start_method
        self.max_workers = max_workers or os.cpu_count()
        self.multiprocessing_module = multiprocessing_module
        logger.debug(
            "init %s Executor with start_method %s and %s workers",
            self.multiprocessing_module, self.start_method, self.max_workers
        )

    def as_completed(
        self,
        func=None,
        iterable=None,
        fargs=None,
        fkwargs=None,
        chunksize=1
    ):
        fargs = fargs or []
        fkwargs = fkwargs or {}
        logger.debug(
            "open multiprocessing.Pool and %s %s workers",
            self.start_method, self.max_workers
        )
        iterable = list(iterable)
        with self.multiprocessing_module.get_context(self.start_method).Pool(
            self.max_workers
        ) as pool:
            logger.debug(
                "submit %s tasks to multiprocessing.Pool.imap_unordered()", len(iterable)
            )
            for i, finished_task in enumerate(pool.imap_unordered(
                partial(_exception_wrapper, func, fargs, fkwargs),
                iterable,
                chunksize=chunksize
            )):
                yield finished_task
                logger.debug("task %s/%s finished", i + 1, len(iterable))
            logger.debug("closing %s and workers", pool)
            pool.close()
            pool.join()
        logger.debug("%s closed", pool)


class FinishedTask():
    """
    Wrapper class to encapsulate exceptions.
    """
    def __init__(self, func, fargs=None, fkwargs=None):
        fargs = fargs or []
        fkwargs = fkwargs or {}
        try:
            self._result = func(*fargs, **fkwargs)
            self._exception = None
        except Exception as e:
            self._result = None
            self._exception = e

    def result(self):
        if self._exception:
            raise self._exception
        else:
            return self._result

    def exception(self):
        return self._exception

    def __repr__(self):
        return "FinishedTask(result=%s, exception=%s)" % (self._result, self._exception)


def _exception_wrapper(func, fargs, fkwargs, i):
    """Wraps function around FinishedTask object."""
    return FinishedTask(func, list(chain([i], fargs)), fkwargs)


###########################
# batch execution options #
###########################

def _run_on_single_tile(process=None, tile=None):
    logger.debug("run process on single tile")
    _, process_info = _execute_write(
        tile,
        process=process,
        mode=process.config.mode
    )
    return process_info


def _run_with_multiprocessing(
    process=None,
    zoom_levels=None,
    multi=None,
    max_chunksize=None,
    multiprocessing_module=None,
    multiprocessing_start_method=None
):
    logger.debug("run concurrently")
    num_processed = 0
    total_tiles = process.count_tiles(min(zoom_levels), max(zoom_levels))
    logger.debug("run process on %s tiles using %s workers", total_tiles, multi)
    with Timer() as t:
        executor = Executor(
            max_workers=multi,
            start_method=multiprocessing_start_method,
            multiprocessing_module=multiprocessing_module
        )
        write_in_parent = True

        # for output drivers requiring writing data in parent process
        if write_in_parent:
            for zoom in zoom_levels:
                for task in executor.as_completed(
                    func=_execute,
                    iterable=(
                        (
                            process_tile,
                            (
                                process.config.mode == "continue" and
                                process.config.output.tiles_exist(process_tile)
                            )
                        )
                        for process_tile in process.get_process_tiles(zoom)
                    ),
                    fkwargs=dict(
                        process=process,
                        mode=process.config.mode,
                        check_existing_tiles=False
                    )
                ):
                    output, process_info = task.result()
                    process_info = _write(
                        process_info.tile,
                        process_info=process_info,
                        process=process,
                        output=output
                    )
                    num_processed += 1
                    logger.info("tile %s/%s finished", num_processed, total_tiles)
                    yield process_info

        # for output drivers which can write data in child processes
        else:
            for zoom in zoom_levels:
                for task in executor.as_completed(
                    func=_execute_write,
                    iterable=process.get_process_tiles(zoom),
                    fkwargs=dict(
                        process=process,
                        mode=process.config.mode
                    )
                ):
                    num_processed += 1
                    logger.info("tile %s/%s finished", num_processed, total_tiles)
                    yield task.result()[1]
    logger.debug("%s tile(s) iterated in %s", str(num_processed), t)


def _run_without_multiprocessing(process=None, zoom_levels=None):
    logger.debug("run sequentially")
    num_processed = 0
    total_tiles = process.count_tiles(min(zoom_levels), max(zoom_levels))
    logger.debug("run process on %s tiles using 1 worker", total_tiles)
    with Timer() as t:
        for zoom in zoom_levels:
            for process_tile in process.get_process_tiles(zoom):
                _, process_info = _execute_write(
                    process_tile,
                    process=process,
                    mode=process.config.mode
                )
                num_processed += 1
                logger.info("tile %s/%s finished", num_processed, total_tiles)
                yield process_info
    logger.info("%s tile(s) iterated in %s", str(num_processed), t)


###############################
# execute and write functions #
###############################

def _execute(
    p, process=None, mode=None, check_existing_tiles=True
):
    if isinstance(p, tuple):
        process_tile, exists = p
    else:
        process_tile, exists = p, False
    logger.debug(
        (process_tile.id, "running on %s" % multiprocessing.current_process().name)
    )

    # skip execution if overwrite is disabled and tile exists
    if (
        check_existing_tiles and (
            mode == "continue" and
            process.config.output.tiles_exist(process_tile)
        ) or exists
    ):
        logger.debug((process_tile.id, "tile exists, skipping"))
        return None, ProcessInfo(
            tile=process_tile,
            processed=False,
            process_msg="output already exists",
            written=False,
            write_msg="nothing written"
        )

    # execute on process tile
    else:
        with Timer() as t:
            try:
                output = process.execute(process_tile, raise_nodata=True)
            except MapcheteNodataTile:
                output = None
        processor_message = "processed in %s" % t
        logger.debug((process_tile.id, processor_message))
        return output, ProcessInfo(
            tile=process_tile,
            processed=True,
            process_msg=processor_message,
            written=None,
            write_msg=None
        )


def _write(
    process_tile, process_info=None, process=None, output=None
):
    if process_info.processed:
        writer_info = process.write(process_tile, output)
        return ProcessInfo(
            tile=process_info.tile,
            processed=process_info.processed,
            process_msg=process_info.process_msg,
            written=writer_info.written,
            write_msg=writer_info.write_msg
        )
    else:
        return process_info


def _execute_write(
    process_tile, process=None, mode=None
):
    output, process_info = _execute(process_tile, process=process, mode=mode)
    return (
        None,
        _write(process_tile, process_info=process_info, process=process, output=output)
    )
