"""Abstraction classes for multiprocessing and distributed processing."""

import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from concurrent.futures._base import CancelledError
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

    def __init__(self, *args, profilers=None, **kwargs):
        self.running_futures = set()
        self.finished_futures = set()
        self.profilers = profilers or []

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
    ) -> Iterator[MFuture]:  # pragma: no cover
        """Submit tasks to executor and start yielding finished futures."""
        ...

    @abstractmethod
    def map(self, *args, **kwargs) -> Iterator[Any]:  # pragma: no cover
        ...

    @abstractmethod
    def _wait(self, *args, **kwargs) -> None:  # pragma: no cover
        ...

    def add_profiler(
        self,
        name: Optional[str] = None,
        decorator: Optional[Callable] = None,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
        profiler: Optional[Profiler] = None,
    ) -> None:
        if profiler:  # pragma: no cover
            self.profilers.append(profiler)
        elif isinstance(name, Profiler):
            self.profilers.append(name)
        else:
            self.profilers.append(
                Profiler(
                    name=name, decorator=decorator, args=args or (), kwargs=kwargs or {}
                )
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
        return func_partial(
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
        mfuture = MFuture.from_future(future, lazy=True, result=result)

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


def run_func_with_profilers(
    func: Callable,
    *args,
    fargs: Optional[tuple] = None,
    fkwargs: Optional[dict] = None,
    profilers: Optional[Iterator[Profiler]] = None,
    **kwargs,
) -> Result:
    """Run function but wrap execution in provided profiler context managers."""
    fargs = fargs or ()
    fargs = args + fargs
    fkwargs = fkwargs or dict()
    fkwargs.update(kwargs)
    profilers = profilers or []
    profilers_output = OrderedDict()
    # append decorators from all profilers
    for profiler in profilers:
        func = profiler.decorator(*profiler.args, **profiler.kwargs)(func)

    # actually run function
    func_output = func(*fargs, **fkwargs)

    # extract profiler results from output
    for idx in list(reversed(range(len(profilers)))):
        profiler = profilers[idx]
        func_output, profiler_output = func_output
        profilers_output[profiler.name] = profiler_output

    return Result(output=func_output, profiling=dict(profilers_output))


def func_partial(
    func: Callable,
    fargs: Optional[tuple] = None,
    fkwargs: Optional[dict] = None,
    profilers: Optional[Iterator[Profiler]] = None,
) -> Callable:
    """Return function parial with activated profilers."""
    return partial(
        run_func_with_profilers,
        func,
        fargs=fargs,
        fkwargs=fkwargs,
        profilers=profilers,
    )
