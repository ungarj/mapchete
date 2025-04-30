import logging
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Tuple

from mapchete.executor.base import ExecutorBase
from mapchete.executor.future import MFuture

logger = logging.getLogger(__name__)


class SequentialExecutor(ExecutorBase):
    """Execute tasks sequentially in single process."""

    def __init__(self, *args, **kwargs):
        """Set attributes."""
        logger.debug("init SequentialExecutor")
        super().__init__(*args, **kwargs)

    def __str__(self) -> str:
        return "<SequentialExecutor>"

    def as_completed(
        self,
        func: Callable,
        iterable: Iterable[Any],
        fargs: Optional[Tuple] = None,
        fkwargs: Optional[Dict[str, Any]] = None,
        item_skip_bool: bool = False,
        **__,
    ) -> Generator[MFuture, None, None]:
        """Yield finished tasks."""
        # before running, make sure cancel signal is False
        self.cancel_signal = False

        for item in iterable:
            if self.cancel_signal:
                logger.debug("executor cancelled")
                return
            # skip task submission if option is activated
            if item_skip_bool:
                item, skip, skip_info = item
                if skip:
                    yield MFuture.skip(skip_info=skip_info, result=item)
                    continue

            # run task and yield future
            yield self._finished_future(
                MFuture.from_func_partial(
                    self.func_partial(func, fargs=fargs, fkwargs=fkwargs), item
                )
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

    def _wait(self):  # pragma: no cover
        return

    def __exit__(self, *args):
        """Exit context manager."""
        logger.debug("SequentialExecutor closed")

    def __repr__(self):  # pragma: no cover
        """Return string representation."""
        return "SequentialExecutor"
