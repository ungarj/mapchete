from __future__ import annotations

import logging
import os
from typing import Any, Callable, Dict, Optional, Protocol, Tuple, Union

from mapchete.errors import MapcheteTaskFailed

FUTURE_TIMEOUT = float(os.environ.get("MP_FUTURE_TIMEOUT", 10))

logger = logging.getLogger(__name__)


class FutureProtocol(Protocol):
    """This protocol is used by both concurrent.futures as well as dask distributed."""

    def result(self, **kwargs) -> Any:
        ...

    def exception(self, **kwargs) -> Union[Exception, None]:
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
    ):
        self._future = future
        self._result = result
        self._exception = exception
        self._cancelled = cancelled
        self.skipped = skipped
        self.skip_info = skip_info
        self.status = status
        self.name = name or repr(self)

    def __repr__(self):  # pragma: no cover
        """Return string representation."""
        return (
            f"<MFuture: type: {type(self._result)}, exception: {type(self._exception)})"
        )

    @staticmethod
    def from_future(
        future: FutureProtocol,
        lazy: bool = True,
        result: Optional[Any] = None,
        timeout: int = FUTURE_TIMEOUT,
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

        if lazy:
            # keep around Future for later and don't call Future.result()
            return MFuture(
                result=result,
                future=future,
                cancelled=future.cancelled(),
                status=status,
                name=name,
            )
        else:
            # immediately fetch Future.result() or use provided result
            try:
                result = result or future.result(timeout=timeout)
                exception = future.exception(timeout=timeout)
            except Exception as exc:
                return MFuture(exception=exc, status=status, name=name)

            return MFuture(result=result, exception=exception, status=status, name=name)

    @staticmethod
    def from_result(result: Any) -> MFuture:
        return MFuture(result=result)

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

    def result(self, timeout: int = FUTURE_TIMEOUT, **kwargs) -> Any:
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

    def cancelled(self) -> bool:  # pragma: no cover
        """Sequential futures cannot be cancelled."""
        return False

    def _populate_from_future(self, timeout: int = FUTURE_TIMEOUT, **kwargs):
        """Fill internal cache with future.result() if future was provided."""
        # only check if there is a cached future but no result nor exception
        if (
            self._future is not None
            and self._result is None
            and self._exception is None
        ):
            try:
                self._result = self._future.result(timeout=timeout, **kwargs)
            except Exception as exc:
                self._exception = exc

    def failed_or_cancelled(self) -> bool:
        """
        Return whether future is failed or cancelled.

        This is a workaround between the slightly different APIs of dask and concurrent.futures.
        It also tries to avoid potentially expensive calls to the dask scheduler.
        """
        if self.status:
            return self.status in ["error", "cancelled"]
        # concurrent.futures futures
        else:
            return self.exception(timeout=FUTURE_TIMEOUT) is not None

    def raise_if_failed(self) -> None:
        """
        Checks whether future contains an exception and raises it as MapcheteTaskFailed.
        """

        # Some exception types such as dask exceptions or generic CancelledErrors indicate that
        # there was an error around the Executor rather than from the future/task itself.
        # Let's directly re-raise these to be more transparent.
        try:
            # when using dask, also directly raise specific dask errors
            from dask.distributed import TimeoutError
            from distributed import CancelledError
            from distributed.comm.core import CommClosedError

            keep_exceptions = (CancelledError, TimeoutError, CommClosedError)
        except ImportError:  # pragma: no cover
            keep_exceptions = (CancelledError,)

        if self.failed_or_cancelled():
            exception = self.exception(timeout=FUTURE_TIMEOUT)

            # sometimes, exceptions are simply empty
            if exception is None:
                raise MapcheteTaskFailed(
                    f"future failed (status: {self.status}), but exception could not be recovered"
                )

            # keep some exceptions as they are
            elif isinstance(exception, keep_exceptions):
                raise exception

            # wrap all other exceptions in a MapcheteTaskFailed
            raise MapcheteTaskFailed(
                f"{self.name} raised a {repr(exception)}"
            ).with_traceback(exception.__traceback__)
