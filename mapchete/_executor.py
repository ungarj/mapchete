"""Abstraction classes for multiprocessing and distributed processing."""

import concurrent.futures
from concurrent.futures._base import CancelledError
from functools import partial
import logging
import multiprocessing
import os
import sys
import warnings

from cached_property import cached_property

from mapchete.errors import JobCancelledError, MapcheteTaskFailed
from mapchete.log import set_log_level
from mapchete._timer import Timer

MULTIPROCESSING_DEFAULT_START_METHOD = "spawn"
FUTURE_TIMEOUT = float(os.environ.get("MP_FUTURE_TIMEOUT", 10))

logger = logging.getLogger(__name__)


class Executor:
    """
    Executor factory for dask and concurrent.futures executor

    Will move into the mapchete core package.
    """

    def __new__(cls, *args, concurrency=None, **kwargs):
        if concurrency == "dask":
            try:
                return DaskExecutor(*args, **kwargs)
            except ImportError as exc:  # pragma: no cover
                raise ImportError(
                    "this feature requires the mapchete[dask] extra"
                ) from exc

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

    cancelled = False
    running_futures = None
    finished_futures = None
    _as_completed = None
    _executor = None
    _executor_cls = None
    _executor_args = ()
    _executor_kwargs = {}

    def __init__(self, *args, **kwargs):
        self.running_futures = set()
        self.finished_futures = set()

    def as_completed(
        self,
        func,
        iterable,
        fargs=None,
        fkwargs=None,
        max_submitted_tasks=500,
        item_skip_bool=False,
        **kwargs,
    ):
        """Submit tasks to executor and start yielding finished futures."""
        try:
            fargs = fargs or ()
            fkwargs = fkwargs or {}
            logger.debug("submitting tasks to executor")
            i = 0
            with Timer() as timer:
                for i, item in enumerate(iterable, 1):
                    if self.cancelled:  # pragma: no cover
                        logger.debug("executor cancelled")
                        return

                    # skip task submission if option is activated
                    if item_skip_bool:
                        item, skip, skip_info = item
                        if skip:
                            yield SkippedFuture(item, skip_info=skip_info)
                            continue

                    # submit task to workers
                    self._submit(func, *[item, *fargs], **fkwargs)

                    # yield finished tasks if any
                    ready = self._ready()
                    if ready:
                        for future in ready:
                            yield self._finished_future(future)

                    # if maximum number of tasks are submitted, wait until the next task is finished
                    if max_submitted_tasks and (
                        len(self.running_futures) >= max_submitted_tasks
                    ):
                        yield self._finished_future(
                            next(self._as_completed(self.running_futures))
                        )

            logger.debug(
                "%s tasks submitted in %s (%s still running)",
                i,
                timer,
                len(self.running_futures),
            )

            # yield remaining futures as they finish
            for future in self._as_completed(self.running_futures):
                yield self._finished_future(future)

        except CancelledError:  # pragma: no cover
            return
        finally:
            # reset so futures won't linger here for next call
            self.running_futures = set()

    def _submit(self, func, *fargs, **fkwargs):
        future = self._executor.submit(func, *fargs, **fkwargs)
        self.running_futures.add(future)
        future.add_done_callback(self._add_to_finished)

    def _ready(self):
        return list(self.finished_futures)

    def _add_to_finished(self, future):
        self.finished_futures.add(future)

    def map(self, func, iterable, fargs=None, fkwargs=None):
        return self._map(func, iterable, fargs=fargs, fkwargs=fkwargs)

    def cancel(self):
        self.cancelled = True
        logger.debug("cancel %s futures...", len(self.running_futures))
        for future in self.running_futures:
            future.cancel()
        logger.debug("%s futures cancelled", len(self.running_futures))
        self.wait()
        # reset so futures won't linger here for next call
        self.running_futures = set()

    def wait(self, raise_exc=False):
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

    def _as_completed(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError()

    def _map(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError()

    def _wait(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError()

    def _finished_future(self, future, result=None, _dask=False):
        """
        Release future from cluster explicitly and wrap result around FinishedFuture object.
        """
        if not _dask:
            self.running_futures.discard(future)
        self.finished_futures.discard(future)

        # raise exception if future errored or was cancelled
        future = future_raise_exception(future)

        # create minimal Future-like object with no references to the cluster
        finished_future = FinishedFuture(future, result=result)

        # explicitly release future
        try:
            future.release()
        except AttributeError:
            pass
        return finished_future

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


class DaskExecutor(_ExecutorBase):
    """Execute tasks using dask cluster."""

    def __init__(
        self,
        *args,
        dask_scheduler=None,
        dask_client=None,
        max_workers=None,
        **kwargs,
    ):
        from dask.distributed import as_completed, Client, LocalCluster

        self._executor_client = dask_client
        self._local_cluster = None
        if self._executor_client:  # pragma: no cover
            logger.debug("using existing dask client: %s", dask_client)
        else:
            local_cluster_kwargs = dict(
                n_workers=max_workers or os.cpu_count(), threads_per_worker=1
            )
            self._executor_cls = Client
            if dask_scheduler is None:
                self._local_cluster = LocalCluster(**local_cluster_kwargs)
            self._executor_kwargs = dict(address=dask_scheduler or self._local_cluster)
            logger.debug(
                "starting dask.distributed.Client with kwargs %s", self._executor_kwargs
            )
        self._ac_iterator = as_completed(
            loop=self._executor.loop, with_results=True, raise_errors=False
        )
        self._submitted = 0
        super().__init__(*args, **kwargs)

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

    def as_completed(
        self,
        func,
        iterable,
        fargs=None,
        fkwargs=None,
        max_submitted_tasks=500,
        item_skip_bool=False,
        chunksize=100,
        **kwargs,
    ):
        """
        Submit tasks to executor and start yielding finished futures.

        Sometimes dask catches an exception (e.g. KilledWorker) and sends a cancel signal to all
        remaining futures. In these cases it can happen that we get a cancelled future before the
        future which contains the KilledWorker (or whatever the source exception was).
        Here we try to iterate through the remaining futures until we can raise the original
        exception as it may contain more relevant information

        Parameters
        ----------
        func : function
            Function to paralellize.
        iterable : iterable
            Iterable with items to parallelize. Each item should be the first argument of
            function.
        fargs : tuple
            Further function arguments.
        fkwargs : dict
            Further function keyword arguments.
        max_submitted_tasks : int
            Make sure that not more tasks are submitted to dask scheduler at once. (default: 500)
        chunksize : int
            Submit tasks in chunks to scheduler.

        Yields
        ------
        finished futures

        """
        from dask.distributed import TimeoutError

        max_submitted_tasks = max_submitted_tasks or 500
        chunksize = chunksize or 100

        try:
            fargs = fargs or ()
            fkwargs = fkwargs or {}
            chunk = []
            for item in iterable:

                # abort if execution is cancelled
                if self.cancelled:  # pragma: no cover
                    logger.debug("executor cancelled")
                    return

                # skip task submission if option is activated
                if item_skip_bool:
                    item, skip, skip_info = item
                    if skip:
                        yield SkippedFuture(item, skip_info=skip_info)
                        continue

                # add processing item to chunk
                chunk.append(item)

                # submit chunk of tasks, if
                # (1) chunksize is reached, or
                # (2) remaining free task spots are less than tasks in chunk
                remaining_spots = max_submitted_tasks - self._submitted
                if len(chunk) % chunksize == 0 or remaining_spots == len(chunk):
                    logger.debug("submitted futures: %s", self._submitted)
                    logger.debug("remaining spots for futures: %s", remaining_spots)
                    logger.debug("current chunk size: %s", len(chunk))
                    self._submit_chunk(
                        chunk=chunk,
                        func=func,
                        fargs=fargs,
                        fkwargs=fkwargs,
                    )
                    chunk = []

                # yield finished tasks, if
                # (1) there are finished tasks available, or
                # (2) maximum allowed number of running tasks is reached
                max_submitted_tasks_reached = self._submitted >= max_submitted_tasks
                if self._ac_iterator.has_ready() or max_submitted_tasks_reached:
                    # yield batch of finished futures
                    # if maximum submitted tasks limit is reached, block call and wait for finished futures
                    logger.debug(
                        "wait for finished tasks: %s", max_submitted_tasks_reached
                    )
                    batch = self._ac_iterator.next_batch(
                        block=max_submitted_tasks_reached
                    )
                    try:
                        yield from self._yield_from_batch(batch)
                    except JobCancelledError:  # pragma: no cover
                        return
                    logger.debug("%s futures still on cluster", self._submitted)

            # submit last chunk of items
            self._submit_chunk(
                chunk=chunk,
                func=func,
                fargs=fargs,
                fkwargs=fkwargs,
            )
            chunk = []
            # yield remaining futures as they finish
            if self._ac_iterator is not None:
                logger.debug("yield %s remaining futures", self._submitted)
                for batch in self._ac_iterator.batches():
                    try:
                        yield from self._yield_from_batch(batch)
                    except JobCancelledError:  # pragma: no cover
                        return

        finally:
            # reset so futures won't linger here for next call
            self.running_futures = set()
            self._ac_iterator.clear()
            self._submitted = 0

    def _submit_chunk(self, chunk=None, func=None, fargs=None, fkwargs=None):
        logger.debug("submit chunk of %s items to cluster", len(chunk))
        futures = self._executor.map(partial(func, *fargs, **fkwargs), chunk)
        self._ac_iterator.update(futures)
        self._submitted += len(futures)

    def _yield_from_batch(self, batch):
        from dask.distributed import TimeoutError
        from distributed.comm.core import CommClosedError

        cancelled_futures = []

        for future, result in batch:
            self._submitted -= 1
            if self.cancelled:  # pragma: no cover
                logger.debug("executor cancelled")
                raise JobCancelledError()
            try:
                yield self._finished_future(future, result, _dask=True)
            except TimeoutError:  # pragma: no cover
                logger.error(
                    "%s: couldn't fetch future result() or exception() in %ss",
                    future,
                    FUTURE_TIMEOUT,
                )
                self._retry(future)
            except CancelledError as exc:  # pragma: no cover
                logger.error("%s got cancelled: %s", future, exc)
                cancelled_futures.append(future)

        if cancelled_futures:  # pragma: no cover
            logger.error("caught %s cancelled_futures", len(cancelled_futures))
            try:
                logger.debug("try to get scheduler logs...")
                logger.debug(
                    "scheduler logs: %s", self._executor.get_scheduler_logs(n=1000)
                )
            except Exception as e:
                logger.exception(e)
            status = self._executor.status
            if status in ("running", "connecting"):
                try:
                    logger.debug("retry %s futures...", len(cancelled_futures))
                    for future in cancelled_futures:
                        self._retry(future)
                except KeyError:
                    raise RuntimeError(
                        f"unable to retry {len(cancelled_futures)} cancelled futures {self._executor} ({status})"
                    )
            else:
                raise RuntimeError(
                    f"client lost connection to scheduler {self._executor} ({status})"
                )

    def _retry(self, future):  # pragma: no cover
        logger.debug("retry future %s", future)
        future.retry()
        self._ac_iterator.add(future)
        self._submitted += 1

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
            logger.debug("closing executor %s...", self._executor)
            try:
                self._executor.close()
            except Exception:  # pragma: no cover
                self._executor.__exit__(*args)
            logger.debug("closed executor %s", self._executor)
        if self._local_cluster:
            logger.debug("closing %s", self._local_cluster)
            self._local_cluster.close()


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
            "init ConcurrentFuturesExecutor using %s with %s workers",
            concurrency,
            self.max_workers,
        )
        super().__init__(*args, **kwargs)

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
        super().__init__(*args, **kwargs)

    def as_completed(
        self, func, iterable, fargs=None, fkwargs=None, item_skip_bool=False, **kwargs
    ):
        """Yield finished tasks."""
        fargs = fargs or []
        fkwargs = fkwargs or {}

        for item in iterable:
            if self.cancelled:
                logger.debug("executor cancelled")
                return
            # skip task submission if option is activated
            if item_skip_bool:
                item, skip, skip_info = item
                if skip:
                    yield SkippedFuture(item, skip_info=skip_info)
                    continue

            # run task and yield future
            yield FakeFuture(func, fargs=[item, *fargs], fkwargs=fkwargs)

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

    def result(self, **kwargs):
        """Return task result."""
        if self._exception:
            logger.exception(self._exception)
            raise self._exception
        return self._result

    def exception(self, **kwargs):
        """Raise task exception if any."""
        return self._exception

    def cancelled(self):  # pragma: no cover
        """Sequential futures cannot be cancelled."""
        return False

    def __repr__(self):  # pragma: no cover
        """Return string representation."""
        return f"<FakeFuture: type: {type(self._result)}, exception: {type(self._exception)})"


class SkippedFuture:
    """Wrapper class to mimick future interface for empty tasks."""

    def __init__(self, result=None, skip_info=None, **kwargs):
        self._result = result
        self.skip_info = skip_info

    def result(self, **kwargs):
        """Only return initial result value."""
        return self._result

    def exception(self, **kwargs):  # pragma: no cover
        """Nothing to raise here."""
        return

    def cancelled(self):  # pragma: no cover
        """Nothing to cancel here."""
        return False

    def __repr__(self):  # pragma: no cover
        """Return string representation."""
        return f"<SkippedFuture: type: {type(self._result)}, exception: {type(self._exception)})"


class FinishedFuture:
    """Wrapper class to mimick future interface."""

    def __init__(self, future=None, result=None):
        """Set attributes."""
        try:
            self._result, self._exception = (
                result or future.result(timeout=FUTURE_TIMEOUT),
                None,
            )
        except Exception as e:  # pragma: no cover
            self._result, self._exception = None, e

    def result(self, **kwargs):
        """Return task result."""
        if self._exception:  # pragma: no cover
            logger.exception(self._exception)
            raise self._exception
        return self._result

    def exception(self, **kwargs):  # pragma: no cover
        """Raise task exception if any."""
        return self._exception

    def cancelled(self):  # pragma: no cover
        """Sequential futures cannot be cancelled."""
        return False

    def __repr__(self):  # pragma: no cover
        """Return string representation."""
        return f"<FinishedFuture: type: {type(self._result)}, exception: {type(self._exception)})"


def future_is_failed_or_cancelled(future):
    """
    Return whether future is failed or cancelled.

    This is a workaround between the slightly different APIs of dask and concurrent.futures.
    It also tries to avoid potentially expensive calls to the dask scheduler.
    """
    # dask futures
    if hasattr(future, "status"):
        return future.status in ["error", "cancelled"]
    # concurrent.futures futures
    else:
        return future.exception(timeout=FUTURE_TIMEOUT) is not None


def future_exception(future):
    """
    Return future exception if future errored or cancelled.

    This is a workaround between the slightly different APIs of dask and concurrent.futures.
    It also tries to avoid potentially expensive calls to the dask scheduler.
    """
    # dask futures
    if hasattr(future, "status"):
        if future.status == "cancelled":  # pragma: no cover
            exception = future.result(timeout=FUTURE_TIMEOUT)
        elif future.status == "error":
            exception = future.exception(timeout=FUTURE_TIMEOUT)
        else:  # pragma: no cover
            exception = None
    else:
        # concurrent.futures futures
        exception = future.exception(timeout=FUTURE_TIMEOUT)

    if exception is None:  # pragma: no cover
        raise TypeError("future %s does not have an exception to raise", future)
    return exception


def future_raise_exception(future, raise_errors=True):
    """
    Checks whether future contains an exception and raises it.
    """
    if raise_errors and future_is_failed_or_cancelled(future):
        exception = future_exception(future)
        future_name = (
            future.key.rstrip("_finished") if hasattr(future, "key") else str(future)
        )
        raise MapcheteTaskFailed(
            f"{future_name} raised a {repr(exception)}"
        ).with_traceback(exception.__traceback__)
    return future
