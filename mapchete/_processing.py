"""Internal processing classes and functions."""

from collections import namedtuple

from itertools import chain
import logging
import multiprocessing
from traceback import format_exc
from typing import Generator

from mapchete.config import get_process_func
from mapchete._executor import DaskExecutor, Executor, SkippedFuture
from mapchete.errors import MapcheteNodataTile, MapcheteProcessException
from mapchete.io import raster
from mapchete._tasks import to_dask_collection, TileTaskBatch, TileTask
from mapchete._timer import Timer

logger = logging.getLogger(__name__)


ProcessInfo = namedtuple(
    "ProcessInfo",
    "tile processed process_msg written write_msg data",
    defaults=(None, None, None, None, None, None),
)


class Job:
    """
    Wraps the output of a processing function into a generator with known length.

    This class also exposes the internal Executor.cancel() function in order to cancel all remaining
    tasks/futures.

    Will move into the mapchete core package.
    """

    def __init__(
        self,
        func: Generator,
        fargs: tuple = None,
        fkwargs: dict = None,
        as_iterator: bool = False,
        tiles_tasks: int = None,
        preprocessing_tasks: int = None,
        executor_concurrency: str = "processes",
        executor_kwargs: dict = None,
    ):
        self.func = func
        self.fargs = fargs or ()
        self.fkwargs = fkwargs or {}
        self.status = "pending"
        self.executor = None
        self.executor_concurrency = executor_concurrency
        self.executor_kwargs = executor_kwargs or {}
        self.tiles_tasks = tiles_tasks or 0
        self.preprocessing_tasks = preprocessing_tasks or 0
        self._total = self.preprocessing_tasks + self.tiles_tasks
        self._as_iterator = as_iterator

        if not as_iterator:
            self._results = list(self._run())

    def _run(self):
        if self._total == 0:
            return
        with Executor(
            concurrency=self.executor_concurrency, **self.executor_kwargs
        ) as self.executor:
            self.status = "running"
            yield from self.func(*self.fargs, executor=self.executor, **self.fkwargs)
            self.status = "finished"

    def set_executor_kwargs(self, executor_kwargs):
        """
        Overwrite default or previously set Executor creation kwargs.

        This only has an effect if Job was initialized with 'as_iterator' and has not yet run.
        """
        self.executor_kwargs = executor_kwargs

    def set_executor_concurrency(self, executor_concurrency):
        """
        Overwrite default or previously set Executor concurrency.

        This only has an effect if Job was initialized with 'as_iterator' and has not yet run.
        """
        self.executor_concurrency = executor_concurrency

    def cancel(self):
        """Cancel all running and pending Job tasks."""
        if self._as_iterator:
            # requires client and futures
            if self.executor is None:  # pragma: no cover
                raise ValueError("nothing to cancel because no executor is running")
            self.executor.cancel()
            self.status = "cancelled"

    def __len__(self):
        return self._total

    def __iter__(self):
        if self._as_iterator:
            yield from self._run()
        else:
            return self._results

    def __repr__(self):  # pragma: no cover
        return f"<{self.status} Job with {self._total} tasks.>"


#######################
# batch preprocessing #
#######################


def _preprocess_task_wrapper(task_tuple):
    task_key, (func, fargs, fkwargs) = task_tuple
    return task_key, func(*fargs, **fkwargs)


def _preprocess(
    tasks,
    process=None,
    dask_scheduler=None,
    dask_max_submitted_tasks=500,
    dask_chunksize=100,
    workers=None,
    multiprocessing_module=None,
    multiprocessing_start_method=None,
    executor=None,
):
    # If preprocessing tasks already finished, don't run them again.
    if process.config.preprocessing_tasks_finished:  # pragma: no cover
        return

    # If an Executor is passed on, don't close after processing. If no Executor is passed on,
    # create one and properly close it afterwards.
    create_executor = executor is None
    executor = executor or Executor(
        max_workers=workers,
        concurrency="dask" if dask_scheduler else "processes",
        start_method=multiprocessing_start_method,
        multiprocessing_module=multiprocessing_module,
        dask_scheduler=dask_scheduler,
    )
    try:
        with Timer() as duration:
            logger.info(
                "run preprocessing on %s tasks using %s workers", len(tasks), workers
            )

            # process all remaining tiles using todo list from before
            for i, future in enumerate(
                executor.as_completed(
                    func=_preprocess_task_wrapper,
                    iterable=list(tasks.items()),
                    max_submitted_tasks=dask_max_submitted_tasks,
                    chunksize=dask_chunksize,
                ),
                1,
            ):
                task_key, result = future.result()
                logger.debug(
                    "preprocessing task %s/%s %s processed successfully",
                    i,
                    len(tasks),
                    task_key,
                )
                process.config.set_preprocessing_task_result(task_key, result)
                yield f"preprocessing task {task_key} finished"
    finally:
        if create_executor:
            executor.close()

    process.config.preprocessing_tasks_finished = True

    logger.info("%s task(s) iterated in %s", str(len(tasks)), duration)


###########################
# batch execution options #
###########################


def _run_on_single_tile(
    executor=None,
    process=None,
    tile=None,
    dask_scheduler=None,
):
    logger.info("run process on single tile")
    create_executor = executor is None
    executor = executor or Executor(
        concurrency="dask" if dask_scheduler else None,
        dask_scheduler=dask_scheduler,
    )
    try:
        return next(
            executor.as_completed(
                func=_execute_and_write,
                iterable=[
                    TileTask(
                        tile=tile,
                        config=process.config,
                        skip=(
                            process.config.mode == "continue"
                            and process.config.output_reader.tiles_exist(tile)
                        ),
                    ),
                ],
                fkwargs=dict(output_writer=process.config.output),
            )
        ).result()
    finally:
        if create_executor:
            executor.close()


def _run_area(
    executor=None,
    process=None,
    zoom_levels=None,
    dask_scheduler=None,
    dask_max_submitted_tasks=500,
    dask_chunksize=100,
    workers=None,
    multiprocessing_module=None,
    multiprocessing_start_method=None,
    skip_output_check=False,
):
    logger.info("run process on area")
    zoom_levels.sort(reverse=True)

    # for output drivers requiring writing data in parent process
    if process.config.output.write_in_parent_process:
        for process_info in _run_multi(
            executor=executor,
            func=_execute,
            zoom_levels=zoom_levels,
            process=process,
            dask_scheduler=dask_scheduler,
            dask_max_submitted_tasks=dask_max_submitted_tasks,
            dask_chunksize=dask_chunksize,
            workers=workers,
            multiprocessing_start_method=multiprocessing_start_method,
            multiprocessing_module=multiprocessing_module,
            write_in_parent_process=True,
            skip_output_check=skip_output_check,
        ):
            yield process_info

    # for output drivers which can write data in child processes
    else:
        for process_info in _run_multi(
            executor=executor,
            func=_execute_and_write,
            fkwargs=dict(output_writer=process.config.output),
            zoom_levels=zoom_levels,
            process=process,
            dask_scheduler=dask_scheduler,
            dask_max_submitted_tasks=dask_max_submitted_tasks,
            dask_chunksize=dask_chunksize,
            workers=workers,
            multiprocessing_start_method=multiprocessing_start_method,
            multiprocessing_module=multiprocessing_module,
            write_in_parent_process=False,
            skip_output_check=skip_output_check,
        ):
            yield process_info


def _filter_skipable(
    process=None, tiles_batches=None, target_set=None, skip_output_check=False
):
    if skip_output_check:  # pragma: no cover
        for batch in tiles_batches:
            for tile in batch:
                yield (tile, False, None)
    else:
        target_set = target_set or set()
        for tile, skip in process.skip_tiles(tiles_batches=tiles_batches):
            if skip and tile not in target_set:
                yield (tile, True, "output already exists")
            else:
                yield (tile, False, None)


def _run_multi(
    executor=None,
    func=None,
    zoom_levels=None,
    process=None,
    dask_scheduler=None,
    dask_max_submitted_tasks=500,
    dask_chunksize=100,
    workers=None,
    multiprocessing_start_method=None,
    multiprocessing_module=None,
    write_in_parent_process=False,
    fkwargs=None,
    skip_output_check=False,
):
    total_tiles = process.count_tiles(min(zoom_levels), max(zoom_levels))
    workers = min([workers, total_tiles])
    num_processed = 0

    # If an Executor is passed on, don't close after processing. If no Executor is passed on,
    # create one and properly close it afterwards.
    create_executor = executor is None
    executor = executor or Executor(
        max_workers=workers,
        concurrency="dask" if dask_scheduler else "processes",
        start_method=multiprocessing_start_method,
        multiprocessing_module=multiprocessing_module,
        dask_scheduler=dask_scheduler,
    )

    try:
        with Timer() as duration:
            logger.info(
                "run process on %s tiles using %s workers on executor %s",
                total_tiles,
                workers,
                executor,
            )
            if isinstance(executor, DaskExecutor):
                for process_info in _run_task_graph(
                    zoom_levels=zoom_levels,
                    executor=executor,
                    func=func,
                    process=process,
                    skip_output_check=skip_output_check,
                    fkwargs=fkwargs,
                    dask_chunksize=dask_chunksize,
                    dask_max_submitted_tasks=dask_max_submitted_tasks,
                    write_in_parent_process=write_in_parent_process,
                ):
                    num_processed += 1
                    logger.debug(
                        "tile %s/%s finished: %s, %s, %s",
                        num_processed,
                        total_tiles,
                        process_info.tile,
                        process_info.process_msg,
                        process_info.write_msg,
                    )
                    yield process_info
            else:
                if process.config.baselevels:
                    f = _run_multi_overviews
                else:
                    f = _run_multi_no_overviews
                for process_info in f(
                    zoom_levels=zoom_levels,
                    executor=executor,
                    func=func,
                    process=process,
                    skip_output_check=skip_output_check,
                    fkwargs=fkwargs,
                    dask_chunksize=dask_chunksize,
                    dask_max_submitted_tasks=dask_max_submitted_tasks,
                    write_in_parent_process=write_in_parent_process,
                ):
                    num_processed += 1
                    logger.debug(
                        "tile %s/%s finished: %s, %s, %s",
                        num_processed,
                        total_tiles,
                        process_info.tile,
                        process_info.process_msg,
                        process_info.write_msg,
                    )
                    yield process_info
    finally:
        if create_executor:
            executor.close()

    logger.info("%s tile(s) iterated in %s", str(num_processed), duration)


def _run_task_graph(
    zoom_levels=None,
    executor=None,
    func=None,
    process=None,
    skip_output_check=None,
    fkwargs=None,
    dask_chunksize=None,
    dask_max_submitted_tasks=None,
    write_in_parent_process=None,
):
    from dask.delayed import delayed
    from distributed import as_completed

    # create one task batch for each processing step

    def _gen_batches():
        # TODO: move to core class
        # TODO: create preprocessing batches
        for zoom in zoom_levels:
            yield TileTaskBatch(
                (
                    TileTask(
                        tile=tile,
                        config=process.config,
                        skip=(
                            process.mode == "continue"
                            and process.config.output_reader.tiles_exist(tile)
                        )
                        if skip_output_check
                        else False,
                    )
                    for tile, skip, process_msg in _filter_skipable(
                        process=process,
                        tiles_batches=process.get_process_tiles(zoom, batch_by="row"),
                        target_set=None,
                        skip_output_check=skip_output_check,
                    )
                ),
                func=func,
                fkwargs=fkwargs,
            )

    # materialize all tasks including dependencies
    with Timer() as t:
        coll = to_dask_collection(_gen_batches())
    logger.debug("%s tasks generated in %s", len(coll), t)

    # send to scheduler
    with Timer() as t:
        futures = executor._executor.compute(coll, optimize_graph=True, traverse=True)
    logger.debug("sent to scheduler in %s", t)

    for future in as_completed(futures):
        futures.remove(future)
        if write_in_parent_process:
            output_data, process_info = future.result()
            process_info = _write(
                process_info=process_info,
                output_data=output_data,
                output_writer=process.config.output,
            )
        # output already has been written, so just use task process info
        else:
            process_info = future.result()
        yield process_info


def _run_multi_overviews(
    zoom_levels=None,
    executor=None,
    func=None,
    process=None,
    skip_output_check=None,
    fkwargs=None,
    dask_chunksize=None,
    dask_max_submitted_tasks=None,
    write_in_parent_process=None,
):
    # here we store the parents of processed tiles so we can update overviews
    # also in "continue" mode in case there were updates at the baselevel
    overview_parents = set()

    for i, zoom in enumerate(zoom_levels):

        # get generator list of tiles, whether they are to be skipped and skip_info
        # from _filter_skipable and pass on to executor
        for future in executor.as_completed(
            func=func,
            iterable=(
                (
                    TileTask(
                        tile=tile,
                        config=process.config,
                        skip=(
                            process.mode == "continue"
                            and process.config.output_reader.tiles_exist(tile)
                        )
                        if skip_output_check
                        else False,
                    ),
                    skip,
                    process_msg,
                )
                for tile, skip, process_msg in _filter_skipable(
                    process=process,
                    tiles_batches=process.get_process_tiles(zoom, batch_by="row"),
                    target_set=(
                        overview_parents if process.config.baselevels and i else None
                    ),
                    skip_output_check=skip_output_check,
                )
            ),
            fkwargs=fkwargs,
            max_submitted_tasks=dask_max_submitted_tasks,
            chunksize=dask_chunksize,
            item_skip_bool=True,
        ):
            # tiles which were not processed
            if isinstance(future, SkippedFuture):
                process_info = ProcessInfo(
                    tile=future.result().tile,
                    processed=False,
                    process_msg=future.skip_info,
                    written=False,
                    write_msg="nothing written",
                )
            # tiles which were processed
            else:
                # trigger output write for driver which require parent process for writing
                if write_in_parent_process:
                    output_data, process_info = future.result()
                    process_info = _write(
                        process_info=process_info,
                        output_data=output_data,
                        output_writer=process.config.output,
                    )

                # output already has been written, so just use task process info
                else:
                    process_info = future.result()
                    # in case of building overviews from baselevels, remember which parent
                    # tile needs to be updated later on
                    if (
                        not skip_output_check
                        and process.config.baselevels
                        and process_info.processed
                        and process_info.tile.zoom > 0
                    ):
                        overview_parents.add(process_info.tile.get_parent())
            try:
                overview_parents.remove(process_info.tile)
            except KeyError:
                pass
            yield process_info


def _run_multi_no_overviews(
    zoom_levels=None,
    executor=None,
    func=None,
    process=None,
    skip_output_check=None,
    fkwargs=None,
    dask_chunksize=None,
    dask_max_submitted_tasks=None,
    write_in_parent_process=None,
):
    # get generator list of tiles, whether they are to be skipped and skip_info
    # from _filter_skipable and pass on to executor
    for future in executor.as_completed(
        func=func,
        iterable=(
            (
                TileTask(
                    tile=tile,
                    config=process.config,
                    skip=(
                        process.mode == "continue"
                        and process.config.output_reader.tiles_exist(tile)
                    )
                    if skip_output_check
                    else False,
                ),
                skip,
                process_msg,
            )
            for tile, skip, process_msg in _filter_skipable(
                process=process,
                tiles_batches=(
                    batch
                    for zoom in zoom_levels
                    for batch in process.get_process_tiles(zoom, batch_by="row")
                ),
                target_set=None,
                skip_output_check=skip_output_check,
            )
        ),
        fkwargs=fkwargs,
        max_submitted_tasks=dask_max_submitted_tasks,
        chunksize=dask_chunksize,
        item_skip_bool=True,
    ):
        # tiles which were not processed
        if isinstance(future, SkippedFuture):
            process_info = ProcessInfo(
                tile=future.result().tile,
                processed=False,
                process_msg=future.skip_info,
                written=False,
                write_msg="nothing written",
            )
        # tiles which were processed
        else:
            # trigger output write for driver which require parent process for writing
            if write_in_parent_process:
                output_data, process_info = future.result()
                process_info = _write(
                    process_info=process_info,
                    output_data=output_data,
                    output_writer=process.config.output,
                )

            # output already has been written, so just use task process info
            else:
                process_info = future.result()
        yield process_info


###############################
# execute and write functions #
###############################


def _execute(tile_process=None, dependencies=None, **_):
    logger.debug(
        (tile_process.tile.id, "running on %s" % multiprocessing.current_process().name)
    )

    # skip execution if overwrite is disabled and tile exists
    if tile_process.skip:
        logger.debug((tile_process.tile.id, "tile exists, skipping"))
        return None, ProcessInfo(
            tile=tile_process.tile,
            processed=False,
            process_msg="output already exists",
            written=False,
            write_msg="nothing written",
        )

    # execute on process tile
    with Timer() as duration:
        try:
            output = tile_process.execute(dependencies=dependencies)
        except MapcheteNodataTile:  # pragma: no cover
            output = "empty"
        except Exception as exc:
            logger.exception(
                "exception caught when processing tile %s", tile_process.tile
            )
            raise exc
    processor_message = "processed in %s" % duration
    logger.debug((tile_process.tile.id, processor_message))
    return output, ProcessInfo(
        tile=tile_process.tile,
        processed=True,
        process_msg=processor_message,
        written=None,
        write_msg=None,
    )


def _write(process_info=None, output_data=None, output_writer=None, **_):
    if process_info.processed:
        try:
            output_data = output_writer.streamline_output(output_data)
        except MapcheteNodataTile:
            output_data = None
        if output_data is None:
            message = "output empty, nothing written"
            logger.debug((process_info.tile.id, message))
            return ProcessInfo(
                tile=process_info.tile,
                processed=process_info.processed,
                process_msg=process_info.process_msg,
                written=False,
                write_msg=message,
            )

        with Timer() as duration:
            output_writer.write(process_tile=process_info.tile, data=output_data)
        message = "output written in %s" % duration
        logger.debug((process_info.tile.id, message))
        return ProcessInfo(
            tile=process_info.tile,
            processed=process_info.processed,
            process_msg=process_info.process_msg,
            written=True,
            write_msg=message,
            data=output_data,
        )

    return process_info


def _execute_and_write(tile_process=None, output_writer=None, dependencies=None, **_):
    output_data, process_info = _execute(
        tile_process=tile_process, dependencies=dependencies
    )
    return _write(
        process_info=process_info, output_data=output_data, output_writer=output_writer
    )
