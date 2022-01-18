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

from mapchete.log import set_log_level
from mapchete._timer import Timer

MULTIPROCESSING_DEFAULT_START_METHOD = "spawn"

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
    _as_completed = None
    _executor = None
    _executor_cls = None
    _executor_args = ()
    _executor_kwargs = {}

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
                    self.running_futures.add(
                        self._executor.submit(func, *[item, *fargs], **fkwargs)
                    )

                    # yield finished tasks if any
                    ready = list(self._finished_futures())
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

            logger.debug("%s tasks submitted in %s", len(self.running_futures), timer)

            # yield remaining futures as they finish
            for future in self._as_completed(self.running_futures):
                yield self._finished_future(future)

        except CancelledError:  # pragma: no cover
            return
        finally:
            # reset so futures won't linger here for next call
            self.running_futures = set()

    def _finished_futures(self):
        for future in [f for f in self.running_futures if f.done()]:
            yield future

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

    def _finished_future(self, future, result=None):
        """
        Release future from cluster explicitly and wrap result around FinishedFuture object.
        """
        self.running_futures.remove(future)
        if future.exception():  # pragma: no cover
            logger.debug("exception caught in future %s", future)
            raise future.exception()
        # create minimal Future-like object with no references to the cluster
        finished_future = FinishedFuture(future, result=result)
        # explicitly release future
        try:
            future.release()
            # logger.debug("%s released", future)
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
            logger.info("using existing dask client: %s", dask_client)
        else:
            local_cluster_kwargs = dict(
                n_workers=max_workers or os.cpu_count(), threads_per_worker=1
            )
            self._executor_cls = Client
            self._executor_kwargs = dict(
                address=dask_scheduler or LocalCluster(**local_cluster_kwargs),
            )
            logger.info(
                "starting dask.distributed.Client with kwargs %s", self._executor_kwargs
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

    def as_completed(
        self,
        func,
        iterable,
        fargs=None,
        fkwargs=None,
        max_submitted_tasks=500,
        raise_cancelled=False,
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
        raise_cancelled : bool
            If a future contains a CancelledError without the Exectuor having initiated the
            cancellation, the CancelledError will be raised in the end.

        Yields
        ------
        finished futures

        """
        from dask.distributed import as_completed

        max_submitted_tasks = max_submitted_tasks or 1
        chunksize = chunksize or 1

        cancelled_exc = None

        try:
            fargs = fargs or ()
            fkwargs = fkwargs or {}
            ac_iterator = as_completed(loop=self._executor.loop, with_results=True)

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
                remaining_spots = max_submitted_tasks - len(self.running_futures)
                if len(chunk) % chunksize == 0 or remaining_spots == len(chunk):
                    logger.debug(
                        "submitted futures (tracked): %s", len(self.running_futures)
                    )
                    logger.debug("remaining spots for futures: %s", remaining_spots)
                    logger.debug("current chunk size: %s", len(chunk))
                    self._submit_chunk(
                        ac_iterator=ac_iterator,
                        chunk=chunk,
                        func=func,
                        fargs=fargs,
                        fkwargs=fkwargs,
                    )
                    chunk = []

                # yield finished tasks, if
                # (1) there are finished tasks available, or
                # (2) maximum allowed number of running tasks is reached
                max_submitted_tasks_reached = (
                    len(self.running_futures) >= max_submitted_tasks
                )
                if ac_iterator.has_ready() or max_submitted_tasks_reached:
                    # yield batch of finished futures
                    # if maximum submitted tasks limit is reached, block call and wait for finished futures
                    logger.debug(
                        "wait for finished tasks: %s", max_submitted_tasks_reached
                    )
                    batch = ac_iterator.next_batch(block=max_submitted_tasks_reached)
                    logger.debug("%s tasks ready for yielding", len(batch))
                    for future, result in batch:
                        try:
                            yield self._finished_future(future, result)
                        except CancelledError as exc:  # pragma: no cover
                            cancelled_exc = exc
                    logger.debug(
                        "%s futures still on cluster", len(self.running_futures)
                    )

            # submit last chunk of items
            self._submit_chunk(
                ac_iterator=ac_iterator,
                chunk=chunk,
                func=func,
                fargs=fargs,
                fkwargs=fkwargs,
            )
            chunk = []
            # yield remaining futures as they finish
            if ac_iterator is not None:
                logger.debug("yield %s remaining futures", len(self.running_futures))
                for batch in ac_iterator.batches():
                    for future, result in batch:
                        if self.cancelled:  # pragma: no cover
                            logger.debug("executor cancelled")
                            return
                        try:
                            yield self._finished_future(future, result)
                        except CancelledError as exc:  # pragma: no cover
                            cancelled_exc = exc

        finally:
            # reset so futures won't linger here for next call
            self.running_futures = set()

        if cancelled_exc is not None and raise_cancelled:  # pragma: no cover
            raise cancelled_exc

    def _submit_chunk(
        self, ac_iterator=None, chunk=None, func=None, fargs=None, fkwargs=None
    ):
        logger.debug("submit chunk of %s items to cluster", len(chunk))
        futures = self._executor.map(partial(func, *fargs, **fkwargs), chunk)
        ac_iterator.update(futures)
        self.running_futures.update(futures)

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
            "init ConcurrentFuturesExecutor using %s with %s workers",
            concurrency,
            self.max_workers,
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

    def result(self):
        """Return task result."""
        if self._exception:
            logger.exception(self._exception)
            raise self._exception
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


class SkippedFuture:
    """Wrapper class to mimick future interface for empty tasks."""

    def __init__(self, result=None, skip_info=None, **kwargs):
        self._result = result
        self.skip_info = skip_info

    def result(self):
        """Only return initial result value."""
        return self._result

    def exception(self):  # pragma: no cover
        """Nothing to raise here."""
        return

    def cancelled(self):  # pragma: no cover
        """Nothing to cancel here."""
        return False


class FinishedFuture:
    """Wrapper class to mimick future interface."""

    def __init__(self, future, result=None):
        """Set attributes."""
        try:
            self._result, self._exception = result or future.result(), None
        except Exception as e:  # pragma: no cover
            self._result, self._exception = None, e

    def result(self):
        """Return task result."""
        if self._exception:  # pragma: no cover
            logger.exception(self._exception)
            raise self._exception
        return self._result

    def exception(self):  # pragma: no cover
        """Raise task exception if any."""
        return self._exception

    def cancelled(self):  # pragma: no cover
        """Sequential futures cannot be cancelled."""
        return False

    def __repr__(self):  # pragma: no cover
        """Return string representation."""
        return f"FinishedFuture(result={self._result}, exception={self._exception})"
