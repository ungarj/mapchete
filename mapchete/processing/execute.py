import logging
from multiprocessing import current_process
from typing import Iterator, Optional

from mapchete.errors import MapcheteNodataTile
from mapchete.executor import DaskExecutor, ExecutorBase, MFuture
from mapchete.formats.base import OutputDataWriter
from mapchete.processing.tasks import Task, Tasks
from mapchete.processing.types import TaskInfo, default_tile_task_id
from mapchete.timer import Timer

logger = logging.getLogger(__name__)


# TODO: writing in parent process


def single_batch(
    executor: ExecutorBase,
    tasks: Tasks,
    output_writer: Optional[OutputDataWriter] = None,
    write_in_parent_process: bool = False,
    propagate_results: bool = False,
) -> Iterator[TaskInfo]:
    """
    Treat all tasks as from a single batch, i.e. they don't have dependencies.
    """
    if write_in_parent_process:
        for future in executor.as_completed(
            _execute_wrapper, tasks.to_batch(), fkwargs=dict(append_data=True)
        ):
            yield _write_wrapper(
                TaskInfo.from_future(future),
                output_writer=output_writer,
                append_data=propagate_results,
            )
    else:
        for future in executor.as_completed(
            _execute_and_write_wrapper,
            tasks.to_batch(),
            fkwargs=dict(output_writer=output_writer, append_data=propagate_results),
        ):
            yield TaskInfo.from_future(future)


def batches(
    executor: ExecutorBase,
    tasks: Tasks,
    output_writer: Optional[OutputDataWriter] = None,
    write_in_parent_process: bool = False,
    propagate_results: bool = False,
) -> Iterator[TaskInfo]:
    """
    Execute batches in sequential order but tasks within batches don't have any order.
    """
    if write_in_parent_process:
        for batch in tasks.to_batches():
            for future in executor.as_completed(
                _execute_wrapper, batch, fkwargs=dict(append_data=True)
            ):
                yield _write_wrapper(
                    TaskInfo.from_future(future),
                    output_writer=output_writer,
                    append_data=propagate_results,
                )
    else:
        for batch in tasks.to_batches():
            for future in executor.as_completed(
                _execute_and_write_wrapper,
                batch,
                fkwargs=dict(
                    output_writer=output_writer,
                    fkwargs=dict(append_data=propagate_results),
                ),
            ):
                yield TaskInfo.from_future(future)


def dask_graph(
    executor: DaskExecutor,
    tasks: Tasks,
    output_writer: Optional[OutputDataWriter] = None,
    write_in_parent_process: bool = False,
    propagate_results: bool = False,
) -> Iterator[TaskInfo]:
    """
    Tasks share dependencies with each other.
    """
    for future in executor.compute_task_graph(
        tasks.to_dask_graph(),
    ):
        yield TaskInfo.from_future(future)


def _execute_wrapper(
    task: Task, dependencies: Optional[dict] = None, append_data: bool = True
) -> TaskInfo:
    """
    Executes tasks and wraps output in a TaskInfo object.
    """
    logger.debug((task.id, "running on %s" % current_process().name))

    try:
        output = task.execute(dependencies=dependencies)
    except MapcheteNodataTile:  # pragma: no cover
        output = "empty"
    processor_message = "processed successfully"
    if isinstance(output, TaskInfo):
        return output
    logger.debug((task.id, processor_message))
    if hasattr(task, "tile"):
        return TaskInfo(
            id=default_tile_task_id(task.tile),
            tile=task.tile,
            processed=True,
            process_msg=processor_message,
            written=None,
            write_msg=None,
            output=output if append_data else None,
        )
    else:
        return TaskInfo(
            id=task.id,
            processed=True,
            process_msg=processor_message,
            written=None,
            write_msg=None,
            output=output if append_data else None,
        )


def _write_wrapper(
    task_info: TaskInfo, output_writer: OutputDataWriter, append_data: bool = False, **_
) -> TaskInfo:
    """Write output from previous step and return updated TileTaskInfo object."""
    if task_info.processed:
        try:
            output_data = output_writer.streamline_output(task_info.output)
        except MapcheteNodataTile:
            output_data = None
        if output_data is None:
            message = "output empty, nothing written"
            logger.debug((task_info.tile.id, message))
            return TaskInfo(
                id=default_tile_task_id(task_info.tile),
                tile=task_info.tile,
                processed=task_info.processed,
                process_msg=task_info.process_msg,
                written=False,
                write_msg=message,
            )
        with Timer() as duration:
            output_writer.write(process_tile=task_info.tile, data=output_data)
        message = "output written in %s" % duration
        logger.debug((task_info.tile.id, message))
        return TaskInfo(
            id=default_tile_task_id(task_info.tile),
            tile=task_info.tile,
            processed=task_info.processed,
            process_msg=task_info.process_msg,
            written=True,
            write_msg=message,
            output=output_data if append_data else None,
        )

    return task_info


def _execute_and_write_wrapper(
    task: Task,
    output_writer: Optional[OutputDataWriter] = None,
    dependencies: Optional[dict] = None,
    append_data: bool = False,
    **_
) -> TaskInfo:
    """
    Execute tile task and write output in one step.
    """
    task_info = _execute_wrapper(task, dependencies=dependencies)
    if task_info.tile:
        return _write_wrapper(
            task_info=task_info,
            output_writer=output_writer,
            append_data=append_data,
        )
    else:
        return task_info
