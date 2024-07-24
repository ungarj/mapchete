import logging
import os
import uuid
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from typing import Any, Callable, Optional, Tuple, Union

from mapchete.io import copy
from mapchete.path import MPath
from mapchete.pretty import pretty_bytes
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
                "function %s consumed a maximum of %s",
                func,
                pretty_bytes(tracker.max_allocated),
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
        self.raise_exc_multiple_trackers = raise_exc_multiple_trackers
        self._temp_dir = TemporaryDirectory()
        self._temp_file = str(
            MPath(self._temp_dir.name) / f"{os.getpid()}-{uuid.uuid4().hex}.bin"
        )
        self.memray_tracker = memray.Tracker(self._temp_file, follow_fork=True)

    def __str__(self):  # pragma: no cover
        return f"<MemoryTracker max_allocated={pretty_bytes(self.max_allocated)}, total_allocated={pretty_bytes(self.total_allocated)}, allocations={self.allocations}>"

    def __repr__(self):  # pragma: no cover
        return repr(str(self))

    def __enter__(self):
        self._temp_dir.__enter__()
        try:
            if self.memray_tracker:
                self.memray_tracker.__enter__()
        except RuntimeError as exc:  # pragma: no cover
            if self.raise_exc_multiple_trackers:
                raise
            logger.exception(exc)
        return self

    def __exit__(self, *args):
        try:
            from memray import FileReader

            # close memray.Tracker before attempting to read file
            if self.memray_tracker:
                self.memray_tracker.__exit__(*args)
            allocations = list(
                FileReader(self._temp_file).get_high_watermark_allocation_records(
                    merge_threads=True
                )
            )
            self.max_allocated = max(record.size for record in allocations)
            self.total_allocated = sum(record.size for record in allocations)
            self.allocations = len(allocations)
            if self.output_file:
                copy(self._temp_file, self.output_file, overwrite=True)
        finally:
            self._temp_dir.__exit__(*args)
            # we need to set this to None, so MemoryTracker can be serialized
            self.memray_tracker = None
