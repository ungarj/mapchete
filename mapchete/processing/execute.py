import logging
from multiprocessing import current_process
from typing import Iterator, Optional

from mapchete.errors import MapcheteNodataTile
from mapchete.executor import DaskExecutor, ExecutorBase
from mapchete.formats.base import OutputDataWriter
from mapchete.processing.tasks import Tasks, TileTask
from mapchete.processing.types import PreprocessingTaskInfo, TaskInfo, TileTaskInfo
from mapchete.timer import Timer

logger = logging.getLogger(__name__)


# TODO: writing in parent process


def single_batch(
    executor: ExecutorBase,
    tasks: Tasks,
    output_writer: Optional[OutputDataWriter] = None,
) -> Iterator[TaskInfo]:
    for future in executor.as_completed(
        _execute_and_write_wrapper,
        tasks.to_batch(),
        fkwargs=dict(output_writer=output_writer),
    ):
        yield future.result()


def batches(
    executor: ExecutorBase,
    tasks: Tasks,
    output_writer: Optional[OutputDataWriter] = None,
) -> Iterator[TaskInfo]:
    for batch in tasks.to_batches():
        for future in executor.as_completed(
            _execute_and_write_wrapper, batch, fkwargs=dict(output_writer=output_writer)
        ):
            yield future.result()


def dask_graph(
    executor: DaskExecutor,
    tasks: Tasks,
    output_writer: Optional[OutputDataWriter] = None,
) -> Iterator[TaskInfo]:
    for future in executor.compute_task_graph(
        tasks.to_dask_graph(),
    ):
        yield future.result()


def _execute_wrapper(
    task: TileTask, dependencies: Optional[dict] = None, append_data: bool = True
) -> TaskInfo:
    """
    Executes tasks and wraps output in a TaskInfo object.
    """
    logger.debug((task.tile.id, "running on %s" % current_process().name))

    try:
        output = task.execute(dependencies=dependencies)
    except MapcheteNodataTile:  # pragma: no cover
        output = "empty"
    processor_message = "processed successfully"
    logger.debug((task.tile.id, processor_message))
    return TaskInfo(
        tile=task.tile,
        processed=True,
        process_msg=processor_message,
        written=None,
        write_msg=None,
        output=output if append_data else None,
    )


def _write_wrapper(
    task_info: TileTaskInfo,
    output_writer: OutputDataWriter,
    append_data: bool = False,
    **_
) -> TileTaskInfo:
    """Write output from previous step and return updated TileTaskInfo object."""
    if task_info.processed:
        try:
            output_data = output_writer.streamline_output(task_info.output)
        except MapcheteNodataTile:
            output_data = None
        if output_data is None:
            message = "output empty, nothing written"
            logger.debug((task_info.tile.id, message))
            return TileTaskInfo(
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
        return TileTaskInfo(
            tile=task_info.tile,
            processed=task_info.processed,
            process_msg=task_info.process_msg,
            written=True,
            write_msg=message,
            output=output_data if append_data else None,
        )

    return task_info


def _execute_and_write_wrapper(
    task: TileTask,
    output_writer: Optional[OutputDataWriter] = None,
    dependencies: Optional[dict] = None,
    append_data: bool = False,
    **_
) -> TileTaskInfo:
    """
    Execute tile task and write output in one step.
    """
    return _write_wrapper(
        task_info=_execute_wrapper(task, dependencies=dependencies),
        output_writer=output_writer,
        append_data=append_data,
    )
