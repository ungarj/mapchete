import logging
import multiprocessing
from contextlib import ExitStack
from typing import Any, Iterator, Optional

from mapchete.enums import Concurrency
from mapchete.errors import MapcheteNodataTile
from mapchete.executor import DaskExecutor, Executor, ExecutorBase
from mapchete.executor.future import MFuture
from mapchete.executor.types import Profiler
from mapchete.path import batch_sort_property
from mapchete.processing.profilers import preconfigured_profilers
from mapchete.processing.tasks import TileTask, to_dask_collection
from mapchete.processing.types import PreprocessingTaskInfo, TileTaskInfo
from mapchete.tile import BufferedTile
from mapchete.timer import Timer
from mapchete.types import ZoomLevels, ZoomLevelsLike

logger = logging.getLogger(__name__)


# TODO: this function probably better goes to base
def compute(
    process,
    zoom_levels: Optional[ZoomLevelsLike] = None,
    tile: Optional[BufferedTile] = None,
    executor: Optional[ExecutorBase] = None,
    concurrency: Concurrency = Concurrency.processes,
    workers: int = multiprocessing.cpu_count(),
    multiprocessing_start_method: Optional[str] = None,
    skip_output_check: bool = False,
    dask_scheduler: Optional[str] = None,
    dask_compute_graph: bool = True,
    dask_propagate_results: bool = True,
    dask_max_submitted_tasks: bool = 500,
    raise_errors: bool = True,
    with_results: bool = False,
    profiling: bool = False,
    **kwargs,
) -> Iterator[MFuture]:
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
                    dask_scheduler=dask_scheduler,
                )
            )
        logger.info("run process on area")
        duration = exit_stack.enter_context(Timer())

        if tile:
            zoom_levels = ZoomLevels.from_inp(tile.zoom)
        elif zoom_levels is None:  # pragma: no cover
            raise ValueError("either tile or zoom_levels has to be provided")

        profilers = []
        if profiling:
            profilers = preconfigured_profilers
            for profiler in profilers:
                executor.add_profiler(profiler)

        if dask_compute_graph and isinstance(executor, DaskExecutor):
            # TODO optimize memory management, e.g. delete preprocessing tasks from input
            # once the dask graph is ready.
            # materialize all tasks including dependencies
            with Timer() as t:
                dask_collection = to_dask_collection(
                    process.task_batches(
                        zoom=zoom_levels,
                        tile=tile,
                        skip_output_check=skip_output_check,
                        propagate_results=process.config.output.write_in_parent_process
                        or dask_propagate_results,
                        profilers=profilers,
                    )
                )
            logger.debug(
                "dask collection with %s tasks generated in %s", len(dask_collection), t
            )
            for num_processed, future in enumerate(
                _compute_task_graph(
                    dask_collection=dask_collection,
                    executor=executor,
                    with_results=with_results,
                    write_in_parent_process=process.config.output.write_in_parent_process,
                    raise_errors=raise_errors,
                    output_writer=process.config.output,
                ),
                1,
            ):
                logger.debug("task %s finished: %s", num_processed, future)
                if raise_errors:
                    future.raise_if_failed()
                yield future
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
                future.raise_if_failed()
                yield future

    logger.info("computed %s tasks in %s", num_processed, duration)


def _compute_task_graph(
    dask_collection,
    executor: DaskExecutor,
    with_results: bool = False,
    write_in_parent_process: bool = False,
    raise_errors: bool = False,
    output_writer: Optional[Any] = None,
    **kwargs,
) -> Iterator[MFuture]:
    # send task graph to executor and yield as ready
    for future in executor.compute_task_graph(
        dask_collection, with_results=with_results, raise_errors=raise_errors
    ):
        if write_in_parent_process:
            yield MFuture.from_result(
                result=_write(
                    process_info=future.result(),
                    output_writer=output_writer,
                    append_output=True,
                ),
                profiling=future.profiling,
            )
        else:
            yield MFuture.from_future(future)


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
) -> Iterator[MFuture]:
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
            future.raise_if_failed()
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
            future.raise_if_failed()
            yield future

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
            future.raise_if_failed()
            yield future


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
    multiprocessing_start_method=None,
    skip_output_check=False,
):
    logger.info("run process on area")
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
    write_in_parent_process=False,
    fkwargs=None,
    skip_output_check=False,
):
    total_tiles = process.count_tiles(zoom_levels.min, zoom_levels.max)
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
) -> Iterator[MFuture]:
    # here we store the parents of processed tiles so we can update overviews
    # also in "continue" mode in case there were updates at the baselevel
    overview_parents = set()
    for i, zoom in enumerate(zoom_levels.descending()):
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
                    tiles_batches=process.get_process_tiles(
                        zoom,
                        batch_by=batch_sort_property(
                            process.config.output_reader.tile_path_schema
                        ),
                    ),
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
            if future.skipped:
                process_info = TileTaskInfo(
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
            yield MFuture.from_result(result=process_info)


def _preprocess_task_wrapper(task, append_data=True, **kwargs) -> PreprocessingTaskInfo:
    data = task.execute(**kwargs)
    return PreprocessingTaskInfo.from_inp(task.id, data, append_output=append_data)


def _preprocess(
    tasks,
    process=None,
    dask_scheduler=None,
    dask_max_submitted_tasks=None,
    dask_chunksize=None,
    workers=None,
    multiprocessing_start_method=None,
    executor=None,
) -> Iterator[MFuture]:
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
) -> Iterator[MFuture]:
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
                    for zoom in zoom_levels.descending()
                    for batch in process.get_process_tiles(
                        zoom,
                        batch_by=batch_sort_property(
                            process.config.output_reader.tile_path_schema
                        ),
                    )
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
        if future.skipped:
            process_info = TileTaskInfo(
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
        yield MFuture.from_result(result=process_info, profiling=future.profiling)


###############################
# execute and write functions #
###############################


def _execute(
    tile_process=None, dependencies=None, append_data=True, **_
) -> TileTaskInfo:
    logger.debug(
        (tile_process.tile.id, "running on %s" % multiprocessing.current_process().name)
    )

    # skip execution if overwrite is disabled and tile exists
    if tile_process.skip:
        logger.debug((tile_process.tile.id, "tile exists, skipping"))
        return TileTaskInfo(
            tile=tile_process.tile,
            processed=False,
            process_msg="output already exists",
            written=False,
            write_msg="nothing written",
            output=None,
        )

    try:
        output = tile_process.execute(dependencies=dependencies)
    except MapcheteNodataTile:  # pragma: no cover
        output = "empty"
    processor_message = "processed successfully"
    logger.debug((tile_process.tile.id, processor_message))
    return TileTaskInfo(
        tile=tile_process.tile,
        processed=True,
        process_msg=processor_message,
        written=None,
        write_msg=None,
        output=output if append_data else None,
    )


def _write(
    process_info=None, output_writer=None, append_data=False, **_
) -> TileTaskInfo:
    if process_info.processed:
        try:
            output_data = output_writer.streamline_output(process_info.output)
        except MapcheteNodataTile:
            output_data = None
        if output_data is None:
            message = "output empty, nothing written"
            logger.debug((process_info.tile.id, message))
            return TileTaskInfo(
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
        return TileTaskInfo(
            tile=process_info.tile,
            processed=process_info.processed,
            process_msg=process_info.process_msg,
            written=True,
            write_msg=message,
            output=output_data if append_data else None,
        )

    return process_info


def _execute_and_write(
    tile_process=None, output_writer=None, dependencies=None, append_data=False, **_
) -> TileTaskInfo:
    return _write(
        process_info=_execute(tile_process=tile_process, dependencies=dependencies),
        output_writer=output_writer,
        append_data=append_data,
    )
