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

    @staticmethod
    def from_future(future: MFuture) -> TaskInfo:
        result = future.result()
        if isinstance(result, TaskInfo):
            task_info = result
            if future.profiling:
                task_info.profiling = future.profiling
        else:  # pragma: no cover
            task_info = TaskInfo(
                id=future.key if hasattr(future, "key") else future.name,
                processed=True,
                profiling=future.profiling,
            )
        return task_info
