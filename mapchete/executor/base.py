"""Abstraction classes for multiprocessing and distributed processing."""

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from concurrent.futures._base import CancelledError
from functools import partial
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from distributed import Client

from mapchete.executor.future import FutureProtocol, MFuture
from mapchete.executor.types import Profiler, Result

logger = logging.getLogger(__name__)


class ExecutorBase(ABC):
    """Define base methods and properties of executors."""

    cancelled: bool = False
    futures: Set[FutureProtocol]
    profilers: List[Profiler]
    _cached_executor = None
    _executor_cls = None
    _executor_args: Tuple
    _executor_kwargs: Dict[str, Any]

    @abstractmethod
    def as_completed(
        self,
        func: Callable,
        iterable: Iterable,
        fargs: Optional[Tuple] = None,
        fkwargs: Optional[Dict[str, Any]] = None,
        item_skip_bool: bool = False,
        **kwargs,
    ) -> Generator[MFuture, None, None]:  # pragma: no cover
        """Submit tasks to executor and start yielding finished futures."""
        ...

    @abstractmethod
    def map(self, *args, **kwargs) -> Iterable[Any]:  # pragma: no cover
        ...

    @abstractmethod
    def _wait(
        self,
        timeout: Optional[float] = None,
        return_when: Literal["FIRST_COMPLETED", "ALL_COMPLETED"] = "ALL_COMPLETED",
    ) -> None:  # pragma: no cover
        ...

    def add_profiler(
        self,
        profiler: Optional[Profiler] = None,
        name: Optional[str] = None,
        decorator: Optional[Callable] = None,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        if profiler:
            self.profilers.append(profiler)
        elif name is not None and decorator is not None:
            self.profilers.append(
                Profiler(
                    name=name, decorator=decorator, args=args or (), kwargs=kwargs or {}
                )
            )
        else:  # pragma: no cover
            raise ValueError("no Profiler, name or decorator given")

    def cancel(self) -> None:
        self.cancel_signal = True
        logger.debug("cancel %s futures...", len(self.futures))
        for future in self.futures:
            if hasattr(future, "cancel"):
                future.cancel()  # type: ignore
        logger.debug("%s futures cancelled", len(self.futures))
        self.wait()
        # reset so futures won't linger here for next call
        self.futures = set()

    def wait(self, raise_exc: bool = False) -> None:
        if self.futures:
            logger.debug("wait for %s running futures to finish...", len(self.futures))
            try:
                self._wait()
            except CancelledError:  # pragma: no cover
                pass
            except Exception as exc:  # pragma: no cover
                logger.error("exception caught when waiting for futures: %s", str(exc))
                if raise_exc:
                    raise exc

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

    def to_mfuture(
        self,
        future: FutureProtocol,
        result: Optional[Any] = None,
        raise_if_failed: bool = True,
    ) -> MFuture:
        """
        Release future from cluster explicitly and wrap result around MFuture object.
        """
        self.futures.discard(future)

        # create minimal Future-like object with no references to the cluster
        mfuture = MFuture.from_future(future, lazy=True, result=result)

        if raise_if_failed:
            # raise exception if future errored or was cancelled
            mfuture.raise_if_failed()

        return mfuture

    @property
    def _executor(self) -> Union[ThreadPoolExecutor, ProcessPoolExecutor, Client]:
        if self._cached_executor is None:
            if self._executor_cls:
                self._cached_executor = self._executor_cls(
                    *self._executor_args, **self._executor_kwargs
                )
            else:  # pragma: no cover
                raise TypeError("no Executor Class given")
        return self._cached_executor

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, *args):
        """Exit context manager."""
        logger.debug("closing executor %s...", self._executor)
        try:
            if self._cached_executor:
                self._executor.close()  # type: ignore
        except Exception:
            pass
        finally:
            if self._cached_executor:
                self._executor.__exit__(*args)
        logger.debug("closed executor %s", self._executor)

    def __repr__(self):  # pragma: no cover
        return f"<Executor ({self._executor_cls})>"


def run_func_with_profilers(
    func: Callable,
    *args,
    fargs: Optional[tuple] = None,
    fkwargs: Optional[dict] = None,
    profilers: Optional[List[Profiler]] = None,
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

    try:
        # actually run function
        func_output = func(*fargs, **fkwargs)
    except Exception as exception:
        logger.exception(exception)
        raise

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
    profilers: Optional[List[Profiler]] = None,
) -> Callable:
    """Return function parial with activated profilers."""
    return partial(
        run_func_with_profilers,
        func,
        fargs=fargs,
        fkwargs=fkwargs,
        profilers=profilers,
    )


# TypeVar for BaseClass or its subclasses
ExecutorType = TypeVar("ExecutorType", bound=ExecutorBase)
