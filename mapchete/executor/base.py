"""Abstraction classes for multiprocessing and distributed processing."""

import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from concurrent.futures._base import CancelledError
from contextlib import AbstractContextManager, ExitStack
from functools import cached_property, partial
from typing import Any, Callable, Iterator, List, Optional

from mapchete.executor.future import FutureProtocol, MFuture
from mapchete.executor.types import Profiler, Result

logger = logging.getLogger(__name__)


class ExecutorBase(ABC):
    """Define base methods and properties of executors."""

    cancelled: bool = False
    running_futures: set = None
    finished_futures: set = None
    profilers: list = None
    _executor_cls = None
    _executor_args = ()
    _executor_kwargs = {}

    def __init__(self, *args, **kwargs):
        self.running_futures = set()
        self.finished_futures = set()
        self.profilers = []

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

    def add_profiler(
        self,
        name: str,
        ctx: AbstractContextManager,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
    ) -> None:
        self.profilers.append(
            Profiler(name=name, ctx=ctx, args=args or (), kwargs=kwargs or {})
        )

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

    def func_partial(
        self,
        func: Callable,
        fargs: Optional[tuple] = None,
        fkwargs: Optional[dict] = None,
    ) -> Callable:
        return partial(
            run_with_profilers,
            func,
            fargs=fargs,
            fkwargs=fkwargs,
            profilers=self.profilers,
        )

    def _finished_future(
        self, future: FutureProtocol, result: Any = None, _dask: bool = False
    ) -> MFuture:
        """
        Release future from cluster explicitly and wrap result around MFuture object.
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


def run_with_profilers(
    func: Callable,
    item: Any,
    fargs: Optional[tuple] = None,
    fkwargs: Optional[dict] = None,
    profilers: Optional[Iterator[Profiler]] = None,
) -> Result:
    """Run function but wrap execution in provided profiler context managers."""
    fargs = fargs or ()
    fkwargs = fkwargs or dict()
    profilers = profilers or []
    profilers_output = OrderedDict()
    with ExitStack() as stack:
        # enter contexts of all profilers
        for profiler in profilers:
            profilers_output[profiler.name] = stack.enter_context(
                profiler.ctx(*profiler.args, **profiler.kwargs)
            )

        # actually run function
        try:
            output = func(item, *fargs, **fkwargs)
            exception = None
        except Exception as exc:
            output = None
            exception = exc

    for profiler_name, profiler_output in profilers_output.items():
        logger.debug("profiler '%s' returned %s", profiler_name, profiler_output)

    if exception:
        raise exception

    return Result(output=output, exception=exception, profiling=dict(profilers_output))
