"""Abstraction classes for multiprocessing and distributed processing."""

import logging
from abc import ABC, abstractmethod
from concurrent.futures._base import CancelledError
from functools import cached_property
from typing import Any, Iterator, List

from mapchete.executor.future import FutureProtocol, MFuture

logger = logging.getLogger(__name__)


class ExecutorBase(ABC):
    """Define base methods and properties of executors."""

    cancelled = False
    running_futures = None
    finished_futures = None
    _executor_cls = None
    _executor_args = ()
    _executor_kwargs = {}

    def __init__(self, *args, **kwargs):
        self.running_futures = set()
        self.finished_futures = set()

    @abstractmethod
    def as_completed(
        self,
        func,
        iterable,
        fargs=None,
        fkwargs=None,
        max_submitted_tasks=500,
        item_skip_bool=False,
        **kwargs,
    ) -> Iterator[MFuture]:
        """Submit tasks to executor and start yielding finished futures."""
        ...

    @abstractmethod
    def map(self, *args, **kwargs) -> Iterator[Any]:
        ...

    @abstractmethod
    def _wait(self, *args, **kwargs) -> None:
        ...

    def _submit(self, func, *fargs, **fkwargs) -> None:
        future = self._executor.submit(func, *fargs, **fkwargs)
        self.running_futures.add(future)
        future.add_done_callback(self._add_to_finished)

    def _ready(self) -> List[MFuture]:
        return list(self.finished_futures)

    def _add_to_finished(self, future) -> None:
        self.finished_futures.add(future)

    def cancel(self) -> None:
        self.cancel_signal = True
        logger.debug("cancel %s futures...", len(self.running_futures))
        for future in self.running_futures:
            future.cancel()
        logger.debug("%s futures cancelled", len(self.running_futures))
        self.wait()
        # reset so futures won't linger here for next call
        self.running_futures = set()

    def wait(self, raise_exc=False) -> None:
        logger.debug("wait for running futures to finish...")
        try:  # pragma: no cover
            self._wait()
        except CancelledError:  # pragma: no cover
            pass
        except Exception as exc:  # pragma: no cover
            logger.error("exception caught when waiting for futures: %s", str(exc))
            if raise_exc:
                raise exc

    def close(self):  # pragma: no cover
        self.__exit__(None, None, None)

    def _finished_future(
        self, future: FutureProtocol, result: Any = None, _dask: bool = False
    ) -> MFuture:
        """
        Release future from cluster explicitly and wrap result around FinishedFuture object.
        """
        if not _dask:
            self.running_futures.discard(future)
        self.finished_futures.discard(future)
        # create minimal Future-like object with no references to the cluster
        mfuture = MFuture.from_future(future, lazy=False, result=result)

        # raise exception if future errored or was cancelled
        mfuture.raise_if_failed()

        return mfuture

    @cached_property
    def _executor(self):
        return self._executor_cls(*self._executor_args, **self._executor_kwargs)

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, *args):
        """Exit context manager."""
        logger.debug("closing executor %s...", self._executor)
        try:
            self._executor.close()
        except Exception:
            self._executor.__exit__(*args)
        logger.debug("closed executor %s", self._executor)

    def __repr__(self):  # pragma: no cover
        return f"<Executor ({self._executor_cls})>"
