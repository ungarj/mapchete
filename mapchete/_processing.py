"""Internal processing classes and functions."""

from collections import namedtuple
from contextlib import ExitStack
from itertools import chain
import logging
import multiprocessing
import os
from shapely.geometry import mapping
from tilematrix._funcs import Bounds
from traceback import format_exc
from typing import Generator

from mapchete.config import get_process_func
from mapchete._executor import (
    DaskExecutor,
    Executor,
    SkippedFuture,
    FinishedFuture,
    future_raise_exception,
)
from mapchete.errors import MapcheteNodataTile, MapcheteTaskFailed
from mapchete._tasks import to_dask_collection, TileTaskBatch, TileTask, TaskBatch
from mapchete._timer import Timer
from mapchete.validate import validate_zooms

FUTURE_TIMEOUT = float(os.environ.get("MP_FUTURE_TIMEOUT", 10))


logger = logging.getLogger(__name__)


ProcessInfo = namedtuple(
    "ProcessInfo",
    "tile processed process_msg written write_msg data",
    defaults=(None, None, None, None, None, None),
)

TileProcessInfo = namedtuple(
    "TileProcessInfo",
    "tile processed process_msg written write_msg data",
    defaults=(None, None, None, None, None, None),
)

PreprocessingProcessInfo = namedtuple(
    "PreprocessingProcessInfo",
    "task_key processed process_msg written write_msg data",
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
        process_area=None,
        stac_item_path: str = None,
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
        self._process_area = process_area
        self.bounds = Bounds(*process_area.bounds) if process_area is not None else None
        self.stac_item_path = stac_item_path

        if not as_iterator:
            self._results = list(self._run())

    @property
    def __geo_interface__(self):  # pragma: no cover
        if self._process_area is not None:
            return mapping(self._process_area)
        else:
            raise AttributeError(f"{self} has no geo information assigned")

    def _run(self):
        if self._total == 0:
            return
        logger.debug("opening executor for job %s", repr(self))
        with Executor(
            concurrency=self.executor_concurrency, **self.executor_kwargs
        ) as self.executor:
            self.status = "running"
            logger.debug("change of job status: %s", self)
            yield from self.func(*self.fargs, executor=self.executor, **self.fkwargs)
            self.status = "finished"
            logger.debug("change of job status: %s", self)

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


def task_batches(
    process, zoom=None, tile=None, skip_output_check=False, propagate_results=True
):
    """Create task batches for each processing stage."""
    with Timer() as duration:
        # preprocessing tasks
        yield TaskBatch(
            id="preprocessing_tasks",
            tasks=process.config.preprocessing_tasks().values(),
            func=_preprocess_task_wrapper,
        )
    logger.debug("preprocessing tasks batch generated in %s", duration)

    with Timer() as duration:
        if tile:
            zoom_levels = [tile.zoom]
            skip_output_check = True
            tiles = {tile.zoom: [(tile, False)]}
        else:
            zoom_levels = list(
                process.config.zoom_levels if zoom is None else validate_zooms(zoom)
            )
            zoom_levels.sort(reverse=True)
            tiles = {}

            # here we store the parents of tiles about to be processed so we can update overviews
            # also in "continue" mode in case there were updates at the baselevel
            overview_parents = set()
            for i, zoom in enumerate(zoom_levels):
                tiles[zoom] = []

                for tile, skip, _ in _filter_skipable(
                    process=process,
                    tiles_batches=process.get_process_tiles(zoom, batch_by="row"),
                    target_set=(
                        overview_parents if process.config.baselevels and i else None
                    ),
                    skip_output_check=skip_output_check,
                ):
                    tiles[zoom].append((tile, skip))
                    # in case of building overviews from baselevels, remember which parent
                    # tile needs to be updated later on
                    if (
                        not skip_output_check
                        and process.config.baselevels
                        and tile.zoom > 0
                    ):
                        # add parent tile
                        overview_parents.add(tile.get_parent())
                        # we don't need the current tile anymore
                    overview_parents.discard(tile)

        if process.config.output.write_in_parent_process:
            func = _execute
            fkwargs = dict(append_data=propagate_results)
        else:
            func = _execute_and_write
            fkwargs = dict(
                append_data=propagate_results, output_writer=process.config.output
            )

        # tile tasks
        for zoom in zoom_levels:
            yield TileTaskBatch(
                id=f"zoom_{zoom}",
                tasks=(
                    TileTask(
                        tile=tile,
                        config=process.config,
                        skip=(
                            process.config.mode == "continue"
                            and process.config.output_reader.tiles_exist(tile)
                        )
                        if skip_output_check
                        else skip,
                    )
                    for tile, skip in tiles[zoom]
                ),
                func=func,
                fkwargs=fkwargs,
            )
    logger.debug("tile task batches generated in %s", duration)


def compute(
    process,
    zoom=None,
    tile=None,
    executor=None,
    concurrency="processes",
    workers=None,
    dask_scheduler=None,
    multiprocessing_start_method=None,
    multiprocessing_module=None,
    skip_output_check=False,
    dask_compute_graph=True,
    dask_propagate_results=True,
    dask_max_submitted_tasks=500,
    raise_errors=True,
    with_results=False,
    **kwargs,
):
    """Computes all tasks and yields progress."""
    concurrency = "dask" if dask_scheduler else concurrency
    num_processed = 0
    with ExitStack() as exit_stack:
        # create fresh Executor if it was not passed on
        if executor is None:
            executor = exit_stack.enter_context(
                Executor(
                    max_workers=workers,
                    concurrency=concurrency,
                    start_method=multiprocessing_start_method,
                    multiprocessing_module=multiprocessing_module,
                    dask_scheduler=dask_scheduler,
                )
            )

        logger.info("run process on area")
        duration = exit_stack.enter_context(Timer())
        if tile:
            tile = process.config.process_pyramid.tile(*tile)
            zoom_levels = [tile.zoom]
        else:
            zoom_levels = list(
                process.config.zoom_levels if zoom is None else validate_zooms(zoom)
            )
        zoom_levels.sort(reverse=True)
        if dask_compute_graph and isinstance(executor, DaskExecutor):
            for num_processed, future in enumerate(
                _compute_task_graph(
                    executor=executor,
                    process=process,
                    zoom_levels=zoom_levels,
                    tile=tile,
                    skip_output_check=skip_output_check,
                    with_results=with_results,
                    propagate_results=dask_propagate_results,
                    raise_errors=raise_errors,
                ),
                1,
            ):
                logger.debug("task %s finished: %s", num_processed, future)
                yield future_raise_exception(future, raise_errors=raise_errors)
        else:
            for num_processed, future in enumerate(
                _compute_tasks(
                    executor=executor,
                    process=process,
                    zoom_levels=zoom_levels,
                    tile=tile,
                    skip_output_check=skip_output_check,
                    dask_max_submitted_tasks=dask_max_submitted_tasks,
                    **kwargs,
                ),
                1,
            ):
                logger.debug("task %s finished: %s", num_processed, future)
                yield future_raise_exception(future)

    logger.info("computed %s tasks in %s", num_processed, duration)


#######################
# batch preprocessing #
#######################


def _preprocess_task_wrapper(task, append_data=True, **kwargs):
    data = task.execute(**kwargs)
    return PreprocessingProcessInfo(
        task_key=task.id, data=data if append_data else None
    )


def _preprocess(
    tasks,
    process=None,
    dask_scheduler=None,
    dask_max_submitted_tasks=None,
    dask_chunksize=None,
    workers=None,
    multiprocessing_module=None,
    multiprocessing_start_method=None,
    executor=None,
):
    # If preprocessing tasks already finished, don't run them again.
    if process.config.preprocessing_tasks_finished:  # pragma: no cover
        return
    num_processed = 0

    # If an Executor is passed on, don't close after processing. If no Executor is passed on,
    # create one and properly close it afterwards.
    with ExitStack() as exit_stack:
        # create fresh Executor if it was not passed on
        if executor is None:
            executor = exit_stack.enter_context(
                Executor(
                    max_workers=workers,
                    concurrency="dask" if dask_scheduler else "processes",
                    start_method=multiprocessing_start_method,
                    multiprocessing_module=multiprocessing_module,
                    dask_scheduler=dask_scheduler,
                )
            )
        duration = exit_stack.enter_context(Timer())
        logger.info(
            "run preprocessing on %s tasks using %s workers", len(tasks), workers
        )
        for num_processed, future in enumerate(
            executor.as_completed(
                func=_preprocess_task_wrapper,
                iterable=tasks.values(),
                fkwargs=dict(append_data=True),
                max_submitted_tasks=dask_max_submitted_tasks,
                chunksize=dask_chunksize,
            ),
            1,
        ):
            result = future.result()
            logger.debug(
                "preprocessing task %s/%s %s processed successfully",
                num_processed,
                len(tasks),
                result.task_key,
            )
            process.config.set_preprocessing_task_result(result.task_key, result.data)
            yield future
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
    with ExitStack() as exit_stack:
        # create fresh Executor if it was not passed on
        if executor is None:
            executor = exit_stack.enter_context(
                Executor(
                    concurrency="dask" if dask_scheduler else None,
                    dask_scheduler=dask_scheduler,
                )
            )
        logger.info("run process on single tile")
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
        )


def _run_area(
    executor=None,
    process=None,
    zoom_levels=None,
    dask_scheduler=None,
    dask_max_submitted_tasks=None,
    dask_chunksize=None,
    workers=None,
    multiprocessing_module=None,
    multiprocessing_start_method=None,
    skip_output_check=False,
):
    logger.info("run process on area")
    zoom_levels.sort(reverse=True)

    # for output drivers requiring writing data in parent process
    if process.config.output.write_in_parent_process:
        for future in _run_multi(
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
            yield future

    # for output drivers which can write data in child processes
    else:
        for future in _run_multi(
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
            yield future


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
    dask_max_submitted_tasks=None,
    dask_chunksize=None,
    workers=None,
    multiprocessing_start_method=None,
    multiprocessing_module=None,
    write_in_parent_process=False,
    fkwargs=None,
    skip_output_check=False,
):
    zoom_levels.sort(reverse=True)
    total_tiles = process.count_tiles(min(zoom_levels), max(zoom_levels))
    workers = min([workers, total_tiles])
    num_processed = 0

    # If an Executor is passed on, don't close after processing. If no Executor is passed on,
    # create one and properly close it afterwards.
    with ExitStack() as exit_stack:
        # create fresh Executor if it was not passed on
        if executor is None:
            executor = exit_stack.enter_context(
                Executor(
                    max_workers=workers,
                    concurrency="dask" if dask_scheduler else "processes",
                    start_method=multiprocessing_start_method,
                    multiprocessing_module=multiprocessing_module,
                    dask_scheduler=dask_scheduler,
                )
            )
        with Timer() as duration:
            logger.info(
                "run process on %s tiles using %s workers on executor %s",
                total_tiles,
                workers,
                executor,
            )
            if process.config.baselevels:
                f = _run_multi_overviews
            else:
                f = _run_multi_no_overviews
            for num_processed, future in enumerate(
                f(
                    zoom_levels=zoom_levels,
                    executor=executor,
                    func=func,
                    process=process,
                    skip_output_check=skip_output_check,
                    fkwargs=fkwargs,
                    dask_chunksize=dask_chunksize,
                    dask_max_submitted_tasks=dask_max_submitted_tasks,
                    write_in_parent_process=write_in_parent_process,
                ),
                1,
            ):
                logger.debug("task %s finished: %s", num_processed, future)
                yield future

        logger.info("%s tile(s) iterated in %s", str(num_processed), duration)


def _compute_task_graph(
    executor=None,
    process=None,
    skip_output_check=False,
    zoom_levels=None,
    tile=None,
    with_results=False,
    propagate_results=False,
    raise_errors=False,
    **kwargs,
):
    # TODO optimize memory management, e.g. delete preprocessing tasks from input
    # once the dask graph is ready.
    from distributed import as_completed

    # materialize all tasks including dependencies
    with Timer() as t:
        coll = to_dask_collection(
            process.task_batches(
                zoom=zoom_levels,
                tile=tile,
                skip_output_check=skip_output_check,
                propagate_results=True
                if process.config.output.write_in_parent_process
                else propagate_results,
            )
        )
    logger.debug("dask collection with %s tasks generated in %s", len(coll), t)
    # send to scheduler
    with Timer() as t:
        futures = executor._executor.compute(coll, optimize_graph=True, traverse=True)
    logger.debug("%s tasks sent to scheduler in %s", len(futures), t)

    logger.debug("wait for tasks to finish...")
    for batch in as_completed(
        futures,
        with_results=with_results,
        raise_errors=raise_errors,
        loop=executor._executor.loop,
    ).batches():
        for future in batch:
            if process.config.output.write_in_parent_process:
                yield FinishedFuture(
                    result=_write(
                        process_info=future.result(),
                        output_writer=process.config.output,
                        append_output=True,
                    )
                )
            else:
                yield future
            futures.remove(future)


def _compute_tasks(
    executor=None,
    process=None,
    workers=None,
    zoom_levels=None,
    tile=None,
    skip_output_check=False,
    dask_max_submitted_tasks=None,
    dask_chunksize=None,
    **kwargs,
):
    if not process.config.preprocessing_tasks_finished:
        tasks = process.config.preprocessing_tasks()
        logger.info(
            "run preprocessing on %s tasks using %s workers", len(tasks), workers
        )
        # process all remaining tiles using todo list from before
        for future in executor.as_completed(
            func=_preprocess_task_wrapper,
            iterable=tasks.values(),
            fkwargs=dict(append_data=True),
            max_submitted_tasks=dask_max_submitted_tasks,
            chunksize=dask_chunksize,
            **kwargs,
        ):
            future = future_raise_exception(future)
            result = future.result()
            process.config.set_preprocessing_task_result(result.task_key, result.data)
            yield future

    # run single tile
    if tile:
        logger.info("run process on single tile")
        for future in executor.as_completed(
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
        ):
            yield future_raise_exception(future)

    else:
        # for output drivers requiring writing data in parent process
        if process.config.output.write_in_parent_process:
            func = _execute
            fkwargs = dict(append_data=True)
            write_in_parent_process = True

        # for output drivers which can write data in child processes
        else:
            func = _execute_and_write
            fkwargs = dict(append_data=False, output_writer=process.config.output)
            write_in_parent_process = False

        # for outputs which have overviews
        if process.config.baselevels:
            _process_batches = _run_multi_overviews
        # for outputs with no overviews
        else:
            _process_batches = _run_multi_no_overviews

        for future in _process_batches(
            zoom_levels=zoom_levels,
            executor=executor,
            func=func,
            process=process,
            skip_output_check=skip_output_check,
            fkwargs=fkwargs,
            write_in_parent_process=write_in_parent_process,
            dask_max_submitted_tasks=dask_max_submitted_tasks,
            dask_chunksize=dask_chunksize,
            **kwargs,
        ):
            yield future_raise_exception(future)


def _run_multi_overviews(
    zoom_levels=None,
    executor=None,
    func=None,
    process=None,
    skip_output_check=None,
    fkwargs=None,
    dask_max_submitted_tasks=None,
    dask_chunksize=None,
    write_in_parent_process=None,
):
    # here we store the parents of processed tiles so we can update overviews
    # also in "continue" mode in case there were updates at the baselevel
    overview_parents = set()

    for i, zoom in enumerate(zoom_levels):

        logger.debug("sending tasks to executor %s...", executor)
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
                process_info = TileProcessInfo(
                    tile=future.result().tile,
                    processed=False,
                    process_msg=future.skip_info,
                    written=False,
                    write_msg="nothing written",
                )
            # tiles which were processed
            else:
                # trigger output write for driver which require parent process for writing
                # the code coverage below is omitted because we don't usually calculate overviews
                # when writing single files
                if write_in_parent_process:  # pragma: no cover
                    process_info = _write(
                        process_info=future.result(),
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
            overview_parents.discard(process_info.tile)
            yield FinishedFuture(result=process_info)


def _run_multi_no_overviews(
    zoom_levels=None,
    executor=None,
    func=None,
    process=None,
    skip_output_check=None,
    fkwargs=None,
    dask_max_submitted_tasks=None,
    dask_chunksize=None,
    write_in_parent_process=None,
):
    logger.debug("sending tasks to executor %s...", executor)
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
            process_info = TileProcessInfo(
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
                process_info = _write(
                    process_info=future.result(),
                    output_writer=process.config.output,
                )

            # output already has been written, so just use task process info
            else:
                process_info = future.result()
        yield FinishedFuture(result=process_info)


###############################
# execute and write functions #
###############################


def _execute(tile_process=None, dependencies=None, append_data=True, **_):
    logger.debug(
        (tile_process.tile.id, "running on %s" % multiprocessing.current_process().name)
    )

    # skip execution if overwrite is disabled and tile exists
    if tile_process.skip:
        logger.debug((tile_process.tile.id, "tile exists, skipping"))
        return TileProcessInfo(
            tile=tile_process.tile,
            processed=False,
            process_msg="output already exists",
            written=False,
            write_msg="nothing written",
            data=None,
        )

    # execute on process tile
    with Timer() as duration:
        try:
            output = tile_process.execute(dependencies=dependencies)
        except MapcheteNodataTile:  # pragma: no cover
            output = "empty"
    processor_message = "processed in %s" % duration
    logger.debug((tile_process.tile.id, processor_message))
    return TileProcessInfo(
        tile=tile_process.tile,
        processed=True,
        process_msg=processor_message,
        written=None,
        write_msg=None,
        data=output if append_data else None,
    )


def _write(process_info=None, output_writer=None, append_data=False, **_):
    if process_info.processed:
        try:
            output_data = output_writer.streamline_output(process_info.data)
        except MapcheteNodataTile:
            output_data = None
        if output_data is None:
            message = "output empty, nothing written"
            logger.debug((process_info.tile.id, message))
            return TileProcessInfo(
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
        return TileProcessInfo(
            tile=process_info.tile,
            processed=process_info.processed,
            process_msg=process_info.process_msg,
            written=True,
            write_msg=message,
            data=output_data if append_data else None,
        )

    return process_info


def _execute_and_write(
    tile_process=None, output_writer=None, dependencies=None, append_data=False, **_
):
    return _write(
        process_info=_execute(tile_process=tile_process, dependencies=dependencies),
        output_writer=output_writer,
        append_data=append_data,
    )
