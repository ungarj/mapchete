import logging
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Tuple

from mapchete.executor.base import ExecutorBase
from mapchete.executor.future import MFuture
from mapchete.executor.types import Profiler

logger = logging.getLogger(__name__)


class SequentialExecutor(ExecutorBase):
    """Execute tasks sequentially in single process."""

    def __init__(self, *_, profilers: Optional[List[Profiler]] = None, **__):
        """Set attributes."""
        logger.debug("init SequentialExecutor")
        self.profilers = profilers or []
        self.futures = set()

    def __str__(self) -> str:
        return "<SequentialExecutor>"

    def as_completed(
        self,
        func: Callable,
        iterable: Iterable,
        fargs: Optional[Tuple] = None,
        fkwargs: Optional[Dict[str, Any]] = None,
        item_skip_bool: bool = False,
        **__,
    ) -> Generator[MFuture, None, None]:
        """Yield finished tasks."""
        # before running, make sure cancel signal is False
        self.cancel_signal = False

        # extract item skip tuples and make a generator
        item_skip_tuples = (
            ((item, skip_item, skip_info) for item, skip_item, skip_info in iterable)
            if item_skip_bool
            else ((item, False, None) for item in iterable)
        )

        for item, skip_item, skip_info in item_skip_tuples:
            if self.cancel_signal:
                logger.debug("executor cancelled")
                return

            if skip_item:
                yield MFuture.skip(skip_info=skip_info, result=item)
                continue

            # run task and yield future
            yield self.to_mfuture(
                MFuture.from_func_partial(
                    self.func_partial(func, fargs=fargs, fkwargs=fkwargs), item
                )  # type: ignore
            )

    def map(
        self,
        func: Callable,
        iterable: Iterable[Any],
        fargs: Optional[Tuple] = None,
        fkwargs: Optional[Dict[str, Any]] = None,
    ) -> List[Any]:
        return [
            result.output
            for result in map(
                self.func_partial(func, fargs=fargs, fkwargs=fkwargs), iterable
            )
        ]

    def cancel(self):
        self.cancel_signal = True

    def _wait(self, *_, **__):  # pragma: no cover
        return

    def __exit__(self, *_):
        """Exit context manager."""
        logger.debug("SequentialExecutor closed")

    def __repr__(self):  # pragma: no cover
        """Return string representation."""
        return "SequentialExecutor"
