from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from mapchete.executor.future import MFuture
from mapchete.executor.types import Result
from mapchete.tile import BufferedTile


def default_tile_task_id(tile: BufferedTile) -> str:
    return f"tile_task_z{tile.zoom}-({tile.zoom}-{tile.row}-{tile.col})"


@dataclass
class TaskInfo:
    id: str = None
    processed: bool = False
    process_msg: Optional[str] = None
    written: bool = False
    write_msg: Optional[str] = None
    output: Optional[Any] = None
    tile: Optional[BufferedTile] = None
    profiling: dict = field(default_factory=dict)

    # @staticmethod
    # def from_future(future: MFuture) -> TaskResult:
    #     process_info = future.result()
    #     if isinstance(process_info, PreprocessingTaskResult):
    #         return TaskResult(
    #             id=process_info.task_key,
    #             processed=process_info.processed,
    #             process_msg=process_info.process_msg,
    #             profiling=process_info.profiling or future.profiling,
    #         )
    #     elif isinstance(process_info, TileTaskResult):
    #         tile = process_info.tile
    #         return TaskResult(
    #             id=f"tile-{tile.zoom}-{tile.row}-{tile.col}",
    #             processed=process_info.processed,
    #             process_msg=process_info.process_msg,
    #             profiling=process_info.profiling or future.profiling,
    #         )
    #     else:  # pragma: no cover
    #         raise TypeError(f"unknown process info type: {type(process_info)}")


class TileTaskInfo(TaskInfo):
    tile: BufferedTile

    def __new__(cls, *args, **kwargs):
        # set id property to default if not set
        if kwargs.get("id") is None:
            kwargs.update(id=default_tile_task_id(kwargs.get("tile")))
        obj = object.__new__(cls)
        TaskInfo.__init__(obj, *args, **kwargs)
        return obj


@dataclass
class PreprocessingTaskInfo(TaskInfo):
    @staticmethod
    def from_inp(
        task_key: str, inp: Any, append_output: bool = True
    ) -> PreprocessingTaskInfo:
        if isinstance(inp, TaskInfo):
            return PreprocessingTaskInfo(**inp.__dict__)
        elif isinstance(inp, Result):
            profiling = inp.profiling
            output = inp.output
        return PreprocessingTaskInfo(
            task_key=task_key,
            output=output if append_output else None,
            profiling=profiling,
        )
