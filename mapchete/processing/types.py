from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from mapchete.executor.future import MFuture
from mapchete.executor.types import Result
from mapchete.tile import BufferedTile


@dataclass
class TileProcessInfo:
    tile: BufferedTile
    processed: bool = False
    process_msg: Optional[str] = None
    written: bool = False
    write_msg: Optional[str] = None
    data: Optional[Any] = None
    profiling: dict = field(default_factory=dict)


@dataclass
class PreprocessingProcessInfo:
    task_key: str
    processed: bool = False
    process_msg: Optional[str] = None
    written: bool = False
    write_msg: Optional[str] = None
    data: Optional[Any] = None
    profiling: dict = field(default_factory=dict)

    @staticmethod
    def from_inp(
        task_key: str, inp: Any, append_data: bool = True
    ) -> PreprocessingProcessInfo:
        if isinstance(inp, Result):
            profiling = inp.profiling
            data = inp.output
        return PreprocessingProcessInfo(
            task_key=task_key, data=data if append_data else None, profiling=profiling
        )


@dataclass
class TaskResult:
    id: str
    processed: bool
    process_msg: Optional[str]
    profiling: dict = field(default_factory=dict)
    result: Optional[Any] = None
    tile: Optional[BufferedTile] = None

    @staticmethod
    def from_future(future: MFuture) -> TaskResult:
        process_info = future.result()
        if isinstance(process_info, PreprocessingProcessInfo):
            return TaskResult(
                id=process_info.task_key,
                processed=process_info.processed,
                process_msg=process_info.process_msg,
                profiling=process_info.profiling or future.profiling,
            )
        elif isinstance(process_info, TileProcessInfo):
            tile = process_info.tile
            return TaskResult(
                id=f"tile-{tile.zoom}-{tile.row}-{tile.col}",
                processed=process_info.processed,
                process_msg=process_info.process_msg,
                profiling=process_info.profiling or future.profiling,
            )
        else:  # pragma: no cover
            raise TypeError(f"unknown process info type: {type(process_info)}")
