from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional, Protocol, Tuple, Union

from dask.distributed import TimeoutError
from distributed import CancelledError
from distributed.comm.core import CommClosedError

from mapchete.errors import MapcheteTaskFailed
from mapchete.executor.types import Result
from mapchete.settings import mapchete_options

logger = logging.getLogger(__name__)


class FutureProtocol(Protocol):
    """This protocol is used by both concurrent.futures as well as dask distributed."""

    def result(self, **kwargs) -> Any:  # pragma: no cover
        ...

    def exception(self, **kwargs) -> Union[Exception, None]:  # pragma: no cover
        ...

    def cancelled(self) -> bool:  # pragma: no cover
        ...


class MFuture:
    """
    Enhanced Future class with some convenience features to ship around and check results.
    """

    skipped: bool = False
    skip_info: Optional[Any] = None
    status: Optional[str] = None
    name: Optional[str] = None
    profiling: Optional[dict] = None

    def __init__(
        self,
        future: Optional[FutureProtocol] = None,
        result: Optional[Any] = None,
        exception: Optional[Exception] = None,
        cancelled: bool = False,
        skipped: bool = False,
        skip_info: Optional[Any] = None,
        status: Optional[str] = None,
        name: Optional[str] = None,
        profiling: Optional[dict] = None,
    ):
        self._future = future
        self._set_result(result)
        self.profiling = self.profiling or profiling or dict()
        self._exception = exception
        self._cancelled = cancelled
        self.skipped = skipped
        self.skip_info = skip_info
        self.status = status
        self.name = name or repr(self)

    def __repr__(self):  # pragma: no cover
        """Return string representation."""
        return f"<MFuture: type: {type(self._result)}, exception: {type(self._exception)}, profiling: {self.profiling})"

    @staticmethod
    def from_future(
        future: FutureProtocol,
        lazy: bool = True,
        result: Optional[Any] = None,
        timeout: int = mapchete_options.future_timeout,
    ) -> MFuture:
        # get status and name if possible
        # distributed.Future
        if hasattr(future, "status"):
            status = future.status
        # concurrent.futures.Future
        else:
            status = None
        # distributed.Future
        if hasattr(future, "key"):
            name = future.key.rstrip("_finished")
        # concurrent.futures.Future
        else:
            name = str(future)

        profiling = future.profiling if hasattr(future, "profiling") else {}

        if lazy:
            # keep around Future for later and don't call Future.result()
            return MFuture(
                result=result,
                future=future,
                cancelled=future.cancelled(),
                status=status,
                name=name,
                profiling=profiling,
                exception=future.exception(),
            )
        else:  # pragma: no cover
            # immediately fetch Future.result() or use provided result
            try:
                result = result or future.result(timeout=timeout)
                exception = future.exception(timeout=timeout)
            except Exception as exc:
                return MFuture(
                    exception=exc, status=status, name=name, profiling=profiling
                )
            return MFuture(
                result=result,
                exception=exception,
                status=status,
                name=name,
                profiling=profiling,
            )

    @staticmethod
    def skip(skip_info: Optional[Any] = None, result: Optional[Any] = None) -> MFuture:
        return MFuture(result=result, skip_info=skip_info, skipped=True)

    @staticmethod
    def from_func(
        func: Callable, fargs: Optional[Tuple] = None, fkwargs: Optional[Dict] = None
    ) -> MFuture:
        try:
            return MFuture(result=func(*fargs, **fkwargs))
        except Exception as exc:  # pragma: no cover
            return MFuture(exception=exc)

    @staticmethod
    def from_func_partial(func: Callable, item: Any) -> MFuture:
        try:
            result = func(item)
        except Exception as exc:  # pragma: no cover
            return MFuture(exception=exc)
        return MFuture(result=result.output, profiling=result.profiling)

    def result(self, timeout: int = mapchete_options.future_timeout, **kwargs) -> Any:
        """Return task result."""
        self._populate_from_future(timeout=timeout)

        if self._exception:
            logger.exception(self._exception)
            raise self._exception

        return self._result

    def exception(self, **kwargs) -> Union[Exception, None]:
        """Raise task exception if any."""
        self._populate_from_future(**kwargs)

        return self._exception

    def cancelled(self) -> bool:
        """Sequential futures cannot be cancelled."""
        return self._cancelled or self.status == "cancelled"

    def failed(self) -> bool:
        return (
            self.status == "error"
            # concurrent.futures futures
            or self.exception(timeout=mapchete_options.future_timeout) is not None
        )

    def _populate_from_future(
        self, timeout: int = mapchete_options.future_timeout, **kwargs
    ):
        """Fill internal cache with future.result() if future was provided."""
        # only check if there is a cached future but no result nor exception
        if (
            self._future is not None
            and self._result is None
            and self._exception is None
        ):
            exc = self._future.exception(timeout=timeout)
            if exc:  # pragma: no cover
                self._exception = exc
            else:
                self._set_result(self._future.result(timeout=timeout, **kwargs))

            # delete reference to future so it can be released from the dask cluster
            self._future = None

    def _set_result(self, result: Any) -> None:
        """Look into result and extract task metadata if available."""
        if isinstance(result, Result):
            self._result = result.output
            self._exception = result.exception
            self.profiling = result.profiling
        else:
            self._result = result

    def raise_if_failed(self) -> None:
        """
        Checks whether future contains an exception and raises it as MapcheteTaskFailed.
        """

        # Some exception types such as dask exceptions or generic CancelledErrors indicate that
        # there was an error around the Executor rather than from the future/task itself.
        # Let's directly re-raise these to be more transparent.
        keep_exceptions = (CancelledError, TimeoutError, CommClosedError)

        if self.cancelled():  # pragma: no cover
            try:
                raise self.exception(timeout=mapchete_options.future_timeout)
            except Exception as exc:  # pragma: no cover
                raise CancelledError(
                    f"{self.name} got cancelled (status: {self.status}) but original "
                    f"exception could not be recovered due to {exc}"
                )

        elif self.failed():
            try:
                exception = self.exception(timeout=mapchete_options.future_timeout)
            except Exception:  # pragma: no cover
                exception = None

            # sometimes, exceptions are simply empty or cannot be retreived
            if exception is None:  # pragma: no cover
                raise MapcheteTaskFailed(
                    f"{self.name} failed (status: {self.status}), but exception could "
                    "not be recovered"
                )

            # keep some exceptions as they are
            if isinstance(exception, keep_exceptions):
                raise exception

            # wrap all other exceptions in a MapcheteTaskFailed
            raise MapcheteTaskFailed(
                f"{self.name} raised a {repr(exception)}"
            ).with_traceback(exception.__traceback__)
