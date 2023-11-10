import logging
import os
import uuid
from contextlib import ExitStack
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from typing import Any, Callable, Optional, Tuple, Union

from mapchete.io import copy
from mapchete.path import MPath
from mapchete.types import MPathLike

logger = logging.getLogger(__name__)


@dataclass
class MeasuredMemory:
    max_allocated: int = 0
    total_allocated: int = 0
    allocations: int = 0


def measure_memory(
    add_to_return: bool = True,
    output_file: Optional[MPathLike] = None,
    raise_exc_multiple_trackers: bool = True,
) -> Callable:
    """Memory tracking decorator."""

    def wrapper(func: Callable) -> Callable:
        """Wrap a function."""

        def wrapped_f(*args, **kwargs) -> Union[Any, Tuple[Any, MeasuredMemory]]:
            with MemoryTracker(
                output_file=output_file,
                raise_exc_multiple_trackers=raise_exc_multiple_trackers,
            ) as tracker:
                retval = func(*args, **kwargs)

            result = MeasuredMemory(
                max_allocated=tracker.max_allocated,
                total_allocated=tracker.total_allocated,
                allocations=tracker.allocations,
            )

            if add_to_return:
                return (retval, result)

            logger.info(
                "function %s consumed a maximum of %sMB",
                func,
                round(tracker.max_allocated / 1024 / 1024, 2),
            )
            return retval

        return wrapped_f

    return wrapper


class MemoryTracker:
    """Tracks memory usage inside context."""

    max_allocated: int = 0
    total_allocated: int = 0
    allocations: int = 0
    output_file: Optional[MPath] = None

    def __init__(
        self,
        output_file: Optional[MPathLike] = None,
        raise_exc_multiple_trackers: bool = True,
    ):
        try:
            import memray
        except ImportError:  # pragma: no cover
            raise ImportError("please install memray if you want to use this feature.")
        self.output_file = MPath.from_inp(output_file) if output_file else None
        self._exit_stack = ExitStack()
        self._temp_dir = self._exit_stack.enter_context(TemporaryDirectory())
        self._temp_file = str(
            MPath(self._temp_dir) / f"{os. getpid()}-{uuid.uuid4().hex}.bin"
        )
        try:
            self._memray_tracker = self._exit_stack.enter_context(
                memray.Tracker(self._temp_file, follow_fork=True)
            )
        except RuntimeError as exc:  # pragma: no cover
            if raise_exc_multiple_trackers:
                raise
            self._memray_tracker = None
            logger.exception(exc)

    def __str__(self):  # pragma: no cover
        max_allocated = f"{self.max_allocated / 1024 / 1024:.2f}MB"
        total_allocated = f"{self.total_allocated / 1024 / 1024:.2f}MB"
        return f"<MemoryTracker max_allocated={max_allocated}, total_allocated={total_allocated}, allocations={self.allocations}>"

    def __repr__(self):  # pragma: no cover
        return repr(str(self))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        try:
            try:
                from memray import FileReader
            except ImportError:  # pragma: no cover
                raise ImportError(
                    "please install memray if you want to use this feature."
                )
            # close memray.Tracker before attempting to read file
            if self._memray_tracker:
                self._memray_tracker.__exit__(*args)
            reader = FileReader(self._temp_file)
            allocations = list(
                reader.get_high_watermark_allocation_records(merge_threads=True)
            )
            self.max_allocated = max(record.size for record in allocations)
            self.total_allocated = sum(record.size for record in allocations)
            self.allocations = len(allocations)
            if self.output_file:
                copy(self._temp_file, self.output_file, overwrite=True)
        finally:
            self._exit_stack.__exit__(*args)
            # we need to set this to None, so MemoryTracker can be serialized
            self._memray_tracker = None
