from cached_property import cached_property
import concurrent.futures
from concurrent.futures._base import CancelledError
from functools import partial
from itertools import chain
import logging
import multiprocessing
import os
import sys
import warnings

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


def _raise_future_exception(f):
    if f.exception():  # pragma: no cover
        logger.debug(f"exception caught in future {f}")
        raise f.exception()
    return f


class _ExecutorBase:
    """Define base methods and properties of executors."""

    cancelled = False
    running_futures = None
    _as_completed = None
    _executor = None
    _executor_cls = None
    _executor_args = ()
    _executor_kwargs = {}

    def as_completed(self, func, iterable, fargs=None, fkwargs=None, chunks=100):
        """Submit tasks to executor in chunks and start yielding finished futures after each chunk."""
        try:
            fargs = fargs or ()
            fkwargs = fkwargs or {}
            logger.debug("submitting tasks to executor")
            import time

            for i, item in enumerate(iterable, 1):
                if self.cancelled:  # pragma: no cover
                    logger.debug("cannot submit new tasks as Executor is cancelling.")
                    return
                logger.debug("submit new task...")
                time.sleep(0.1)
                future = self._executor.submit(func, *chain([item], fargs), **fkwargs)
                self.running_futures.add(future)
                # try to yield finished futures after submitting a chunk
                if i % chunks == 0:
                    yield from self._finished_futures()

            # yield remaining futures as they finish
            for future in self._as_completed(self.running_futures):
                yield _raise_future_exception(future)
        except CancelledError:  # pragma: no cover
            return
        finally:
            # reset so futures won't linger here for next call
            self.running_futures = set()

    def _finished_futures(self):
        done = set()
        for future in self.running_futures:
            if future.done():
                yield _raise_future_exception(future)
                done.add(future)
        if done:
            # remove from running futures
            self.running_futures.difference_update(done)

    def map(self, func, iterable, fargs=None, fkwargs=None):
        return self._map(func, iterable, fargs=fargs, fkwargs=fkwargs)

    def cancel(self):
        self.cancelled = True
        logger.debug(f"cancel {len(self.running_futures)} futures...")
        for future in self.running_futures:
            future.cancel()
        logger.debug(f"{len(self.running_futures)} futures cancelled")
        self.wait()
        # reset so futures won't linger here for next call

        self.running_futures = set()

    def wait(self):
        logger.debug("wait for running futures to finish...")
        try:  # pragma: no cover
            self._wait()
        except CancelledError:
            pass

    def close(self):  # pragma: no cover
        self.__exit__(None, None, None)

    def _as_completed(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError()

    def _map(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError()

    def _wait(self, *args, **kwargs):  # pragma: no cover
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

        self.running_futures = set()
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

    def _map(self, func, iterable, fargs=None, fkwargs=None):
        fargs = fargs or []
        fkwargs = fkwargs or {}
        return [
            f.result()
            for f in self._executor.map(partial(func, *fargs, **fkwargs), iterable)
        ]

    def _wait(self):
        from dask.distributed import wait

        wait(self.running_futures)

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
        multiprocessing_start_method=None,
        **kwargs,
    ):
        """Set attributes."""
        start_method = (
            multiprocessing_start_method or MULTIPROCESSING_DEFAULT_START_METHOD
        )
        self.max_workers = max_workers or os.cpu_count()
        self.running_futures = set()
        self._executor_kwargs = dict(
            max_workers=self.max_workers,
        )
        if sys.version_info >= (3, 7):
            self._executor_kwargs.update(
                initializer=set_log_level,
                initargs=(logger.getEffectiveLevel(),),
            )
        else:  # pragma: no cover
            warnings.warn(UserWarning("worker logs are not available on python<3.7"))
        if concurrency == "processes":
            self._executor_cls = concurrent.futures.ProcessPoolExecutor
            if sys.version_info >= (3, 7):
                self._executor_kwargs.update(
                    mp_context=multiprocessing.get_context(method=start_method)
                )
                if start_method != "spawn":  # pragma: no cover
                    warnings.warn(
                        UserWarning(
                            "using a start method other than 'spawn' can cause mapchete to hang"
                        )
                    )
            else:  # pragma: no cover
                warnings.warn(
                    UserWarning(
                        "multiprocessing start method cannot be set on python<3.7"
                    )
                )
        elif concurrency == "threads":
            self._executor_cls = concurrent.futures.ThreadPoolExecutor
        else:  # pragma: no cover
            raise ValueError("concurrency must either be 'processes' or 'threads'")
        logger.debug(
            f"init ConcurrentFuturesExecutor using {concurrency} with {self.max_workers} workers"
        )

    def _wait(self):
        concurrent.futures.wait(self.running_futures)

    def _as_completed(self, futures):
        """Yield finished tasks."""
        for future in concurrent.futures.as_completed(futures):
            yield future

    def _map(self, func, iterable, fargs=None, fkwargs=None):
        fargs = fargs or []
        fkwargs = fkwargs or {}
        return list(self._executor.map(partial(func, *fargs, **fkwargs), iterable))


class SequentialExecutor(_ExecutorBase):
    """Execute tasks sequentially in single process."""

    def __init__(self, *args, **kwargs):
        """Set attributes."""
        logger.debug("init SequentialExecutor")
        self.running_futures = set()

    def as_completed(self, func, iterable, fargs=None, fkwargs=None):
        """Yield finished tasks."""
        fargs = fargs or []
        fkwargs = fkwargs or {}
        for i in iterable:
            if self.cancelled:
                return
            yield FakeFuture(func, fargs=[i, *fargs], fkwargs=fkwargs)

    def _map(self, func, iterable, fargs=None, fkwargs=None):
        fargs = fargs or []
        fkwargs = fkwargs or {}
        return list(map(partial(func, *fargs, **fkwargs), iterable))

    def cancel(self):
        self.cancelled = True

    def _wait(self):  # pragma: no cover
        return

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
