from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from mapchete.executor.future import MFuture
from mapchete.settings import mapchete_options
from mapchete.tile import BufferedTile


def default_tile_task_id(tile: BufferedTile) -> str:
    return f"tile_task_z{tile.zoom}-({tile.zoom}-{tile.row}-{tile.col})"


@dataclass
class TaskInfo:
    """Storage of metadata from finished task."""

    id: str = None
    processed: bool = False
    process_msg: Optional[str] = None
    written: bool = False
    write_msg: Optional[str] = None
    output: Optional[Any] = None
    tile: Optional[BufferedTile] = None
    profiling: dict = field(default_factory=dict)

    @staticmethod
    def from_future(future: MFuture) -> LazyTaskInfo:
        return LazyTaskInfo(future)


class LazyTaskInfo(TaskInfo):
    """Lazy storage of task metadata using a future."""

    _future: MFuture
    _result: Any = None
    _result_is_set: bool = False

    def __init__(self, future: MFuture):
        self._future = future

    def _set_result(self):
        if self._result_is_set:
            return

        result = self._future.result(timeout=mapchete_options.future_timeout)

        if isinstance(result, TaskInfo):
            task_info = result
            self._id = task_info.id
            self._processed = task_info.processed
            self._process_msg = task_info.process_msg
            self._written = task_info.written
            self._write_msg = task_info.write_msg
            self._output = task_info.output
            self._tile = task_info.tile
            self._profiling = task_info.profiling
            if self._future.profiling:
                self._profiling = self._future.profiling
        else:  # pragma: no cover
            self._id = (
                self._future.key if hasattr(self._future, "key") else self._future.name
            )
            self._processed = True
            self._profiling = self._future.profiling

        self._result_is_set = True
        self._future = None

    @property
    def id(self) -> str:  # pragma: no cover
        self._set_result()
        return self._id

    @property
    def processed(self) -> bool:  # pragma: no cover
        self._set_result()
        return self._processed

    @property
    def process_msg(self) -> Optional[str]:  # pragma: no cover
        self._set_result()
        return self._process_msg

    @property
    def written(self) -> bool:  # pragma: no cover
        self._set_result()
        return self._written

    @property
    def write_msg(self) -> Optional[str]:  # pragma: no cover
        self._set_result()
        return self._write_msg

    @property
    def output(self) -> Optional[Any]:  # pragma: no cover
        self._set_result()
        return self._output

    @property
    def tile(self) -> Optional[BufferedTile]:  # pragma: no cover
        self._set_result()
        return self._tile

    @property
    def profiling(self) -> dict:  # pragma: no cover
        self._set_result()
        return self._profiling
