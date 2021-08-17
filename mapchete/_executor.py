from cached_property import cached_property
import concurrent.futures
from functools import partial
from itertools import chain
import logging
import multiprocessing
import os

from mapchete.config import MULTIPROCESSING_DEFAULT_START_METHOD
from mapchete.log import set_log_level


logger = logging.getLogger(__name__)


class Executor:
    """
    Executor factory for dask and concurrent.futures executor

    Will move into the mapchete core package.
    """

    def __new__(self, *args, concurrency=None, **kwargs):
        if concurrency == "dask":
            try:
                return DaskExecutor(*args, **kwargs)
            except ImportError as e:  # pragma: no cover
                raise ImportError(
                    f"this feature requires the mapchete[dask] extra: {e}"
                )
        elif concurrency is None or kwargs.get("max_workers") == 1:
            return SequentialExecutor(*args, **kwargs)
        elif concurrency in ["processes", "threads"]:
            return ConcurrentFuturesExecutor(*args, concurrency=concurrency, **kwargs)
        else:  # pragma: no cover
            raise ValueError(
                f"concurrency must be one of None, 'processes', 'threads' or 'dask', not {concurrency}"
            )


class _ExecutorBase:
    """Define base methods and properties of executors."""

    futures = None
    _as_completed = None
    _executor = None
    _executor_cls = None
    _executor_args = ()
    _executor_kwargs = {}

    def as_completed(self, func, iterable, fargs=None, fkwargs=None):
        """Yield finished tasks."""
        fargs = fargs or ()
        fkwargs = fkwargs or {}
        logger.debug("submitting tasks to executor")
        futures = [
            self._executor.submit(func, *chain([item], fargs), **fkwargs)
            for item in iterable
        ]
        self.futures.extend(futures)
        logger.debug(f"added {len(futures)} tasks")
        for future in self._as_completed(futures):
            yield future

    def cancel(self):
        logger.debug(f"cancel {len(self.futures)} futures...")
        for future in self.futures:
            future.cancel()
        logger.debug(f"{len(self.futures)} futures cancelled")

    def close(self):  # pragma: no cover
        self.__exit__(None, None, None)

    def _as_completed(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError()

    @cached_property
    def _executor(self):
        return self._executor_cls(*self._executor_args, **self._executor_kwargs)

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, *args):
        """Exit context manager."""
        logger.debug(f"closing executor {self._executor}...")
        try:
            self._executor.close()
        except Exception:
            self._executor.__exit__(*args)
        logger.debug(f"closed executor {self._executor}")

    def __repr__(self):  # pragma: no cover
        return f"<Executor ({self._executor_cls})>"


class DaskExecutor(_ExecutorBase):
    """Execute tasks using dask cluster."""

    def __init__(
        self,
        *args,
        address=None,
        dask_scheduler=None,
        dask_client=None,
        max_workers=None,
        **kwargs,
    ):
        from dask.distributed import Client, LocalCluster

        self.futures = []
        self._executor_client = dask_client
        if self._executor_client:  # pragma: no cover
            logger.debug(f"using existing dask client: {dask_client}")
        else:
            local_cluster_kwargs = dict(
                n_workers=max_workers or os.cpu_count(), threads_per_worker=1
            )
            self._executor_cls = Client
            self._executor_kwargs = dict(
                address=dask_scheduler or LocalCluster(**local_cluster_kwargs),
            )
            logger.debug(
                f"starting dask.distributed.Client with kwargs {self._executor_kwargs}"
            )

    def cancel(self):
        logger.debug(f"cancel {len(self.futures)} futures...")
        for future in self.futures:
            future.cancel()
        logger.debug(f"{len(self.futures)} futures cancelled")

    def _as_completed(self, futures):
        from dask.distributed import as_completed

        if futures:
            for future in as_completed(futures):
                yield future

    @cached_property
    def _executor(self):
        return self._executor_client or self._executor_cls(
            *self._executor_args, **self._executor_kwargs
        )

    def __exit__(self, *args):
        """Exit context manager."""
        if self._executor_client:  # pragma: no cover
            logger.debug("client not closing as it was passed on as kwarg")
        else:
            logger.debug(f"closing executor {self._executor}...")
            try:
                self._executor.close()
            except Exception:  # pragma: no cover
                self._executor.__exit__(*args)
            logger.debug(f"closed executor {self._executor}")


class ConcurrentFuturesExecutor(_ExecutorBase):
    """Execute tasks using concurrent.futures."""

    def __init__(
        self,
        *args,
        max_workers=None,
        concurrency="processes",
        **kwargs,
    ):
        """Set attributes."""

        self.max_workers = max_workers or os.cpu_count()
        self.futures = []
        self._executor_kwargs = dict(
            max_workers=self.max_workers,
            initializer=set_log_level,
            initargs=(logger.getEffectiveLevel(),),
        )
        if concurrency == "processes":
            self._executor_cls = concurrent.futures.ProcessPoolExecutor
            start_method = (
                kwargs.get("multiprocessing_start_method")
                or MULTIPROCESSING_DEFAULT_START_METHOD
            )
            self._executor_kwargs.update(
                mp_context=multiprocessing.get_context(method=start_method)
            )
        elif concurrency == "threads":
            self._executor_cls = concurrent.futures.ThreadPoolExecutor
        else:  # pragma: no cover
            raise ValueError("concurrency must either be 'processes' or 'threads'")
        logger.debug(
            f"init ConcurrentFuturesExecutor using {concurrency} with {self.max_workers} workers"
        )

    def _as_completed(self, futures):
        """Yield finished tasks."""
        for future in concurrent.futures.as_completed(futures):
            yield future


class SequentialExecutor(_ExecutorBase):
    """Execute tasks sequentially in single process."""

    def __init__(self, *args, **kwargs):
        """Set attributes."""
        logger.debug("init SequentialExecutor")
        self.futures = []
        self._cancel = False

    def as_completed(self, func, iterable, fargs=None, fkwargs=None):
        """Yield finished tasks."""
        fargs = fargs or []
        fkwargs = fkwargs or {}
        for i in iterable:
            if self._cancel:
                return
            yield FakeFuture(func, fargs=[i, *fargs], fkwargs=fkwargs)

    def cancel(self):
        self._cancel = True

    def __exit__(self, *args):
        """Exit context manager."""
        logger.debug("SequentialExecutor closed")

    def __repr__(self):  # pragma: no cover
        """Return string representation."""
        return "SequentialExecutor"


class FakeFuture:
    """Wrapper class to mimick future interface."""

    def __init__(self, func, fargs=None, fkwargs=None):
        """Set attributes."""
        fargs = fargs or []
        fkwargs = fkwargs or {}
        try:
            self._result, self._exception = func(*fargs, **fkwargs), None
        except Exception as e:  # pragma: no cover
            self._result, self._exception = None, e

    def result(self):
        """Return task result."""
        if self._exception:
            logger.exception(self._exception)
            raise self._exception
        else:
            return self._result

    def exception(self):
        """Raise task exception if any."""
        return self._exception

    def cancelled(self):  # pragma: no cover
        """Sequential futures cannot be cancelled."""
        return False

    def __repr__(self):  # pragma: no cover
        """Return string representation."""
        return f"FakeFuture(result={self._result}, exception={self._exception})"
