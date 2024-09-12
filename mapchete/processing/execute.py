import logging
from functools import partial
from multiprocessing import current_process
from typing import Callable, Iterator, Optional

from mapchete.errors import MapcheteNodataTile
from mapchete.executor import DaskExecutor, ExecutorBase
from mapchete.formats.base import OutputDataWriter
from mapchete.processing.tasks import Task, Tasks
from mapchete.processing.types import TaskInfo, default_tile_task_id
from mapchete.timer import Timer

logger = logging.getLogger(__name__)


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
    task_wrapper = get_task_wrapper(
        write_in_parent_process=write_in_parent_process,
        output_writer=output_writer,
        propagate_results=propagate_results,
    )

    for future in executor.as_completed(task_wrapper, tasks.to_batch()):
        task_info = TaskInfo.from_future(future)

        if write_in_parent_process:
            task_info = write_wrapper(
                task_info,
                output_writer=output_writer,
                append_data=propagate_results,
            )

        yield task_info


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
    preprocessing_tasks_results = {}

    task_wrapper = get_task_wrapper(
        write_in_parent_process=write_in_parent_process,
        output_writer=output_writer,
        propagate_results=propagate_results,
    )

    for batch in tasks.to_batches():
        # preprocessing task results have to be appended to each tile task batch:
        if batch.id != "preprocessing_tasks":
            for task in batch:
                for id, result in preprocessing_tasks_results.items():
                    task.add_dependency(id, result)

        for future in executor.as_completed(task_wrapper, batch):
            task_info = TaskInfo.from_future(future)

            if write_in_parent_process:
                task_info = write_wrapper(
                    task_info,
                    output_writer=output_writer,
                    append_data=propagate_results,
                )

            # remember preprocessing task result
            if task_info.tile is None:
                preprocessing_tasks_results[task_info.id] = task_info.output

            yield task_info


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
    task_wrapper = get_task_wrapper(
        write_in_parent_process=write_in_parent_process,
        output_writer=output_writer,
        propagate_results=propagate_results,
    )

    for future in executor.compute_task_graph(
        tasks.to_dask_graph(
            preprocessing_task_wrapper=task_wrapper,
            tile_task_wrapper=task_wrapper,
        ),
    ):
        task_info = TaskInfo.from_future(future)
        if write_in_parent_process:
            yield write_wrapper(
                task_info,
                output_writer=output_writer,
                append_data=propagate_results,
            )
        else:
            yield task_info


def get_task_wrapper(
    write_in_parent_process: bool = False,
    output_writer: Optional[OutputDataWriter] = None,
    propagate_results: bool = False,
) -> Callable:
    """Return a partially initialized wrapper function for task."""
    if write_in_parent_process:
        return partial(execute_wrapper, append_data=True)
    else:
        return partial(
            execute_and_write_wrapper,
            output_writer=output_writer,
            append_data=propagate_results,
        )


def execute_wrapper(
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

    logger.debug((task.id, processor_message))

    if task.tile:
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


def write_wrapper(
    task_info: TaskInfo, output_writer: OutputDataWriter, append_data: bool = False, **_
) -> TaskInfo:
    """Write output from previous step and return updated TileTaskInfo object."""
    if task_info.processed and task_info.tile:
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


def execute_and_write_wrapper(
    task: Task,
    output_writer: Optional[OutputDataWriter] = None,
    dependencies: Optional[dict] = None,
    append_data: bool = False,
    **_,
) -> TaskInfo:
    """
    Execute tile task and write output in one step.
    """
    return write_wrapper(
        task_info=execute_wrapper(task, dependencies=dependencies),
        output_writer=output_writer,
        append_data=append_data,
    )
