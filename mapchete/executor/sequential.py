import logging
from functools import partial
from typing import Any, Iterator, List

from mapchete.executor.base import ExecutorBase
from mapchete.executor.future import MFuture

logger = logging.getLogger(__name__)


class SequentialExecutor(ExecutorBase):
    """Execute tasks sequentially in single process."""

    def __init__(self, *args, **kwargs):
        """Set attributes."""
        logger.debug("init SequentialExecutor")
        super().__init__(*args, **kwargs)

    def as_completed(
        self, func, iterable, fargs=None, fkwargs=None, item_skip_bool=False, **kwargs
    ) -> Iterator[MFuture]:
        """Yield finished tasks."""
        fargs = fargs or []
        fkwargs = fkwargs or {}

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
            yield MFuture.from_func(func, fargs=(item, *fargs), fkwargs=fkwargs)

    def map(self, func, iterable, fargs=None, fkwargs=None) -> List[Any]:
        fargs = fargs or []
        fkwargs = fkwargs or {}
        return list(map(partial(func, *fargs, **fkwargs), iterable))

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
