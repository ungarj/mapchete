import logging
import os
from functools import cached_property, partial
from typing import Any, Iterator, List

from mapchete.errors import JobCancelledError
from mapchete.executor.base import ExecutorBase
from mapchete.executor.future import MFuture

logger = logging.getLogger(__name__)


class DaskExecutor(ExecutorBase):
    """Execute tasks using dask cluster."""

    def __init__(
        self,
        *args,
        dask_scheduler=None,
        dask_client=None,
        max_workers=None,
        **kwargs,
    ):
        from dask.distributed import Client, LocalCluster, as_completed

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

    def map(self, func, iterable, fargs=None, fkwargs=None) -> List[Any]:
        fargs = fargs or []
        fkwargs = fkwargs or {}
        return [
            f.result()
            for f in self._executor.map(partial(func, *fargs, **fkwargs), iterable)
        ]

    def _wait(self):
        from dask.distributed import wait

        wait(self.running_futures)

    def _as_completed(self, *args, **kwargs) -> Iterator[MFuture]:
        return

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
    ) -> Iterator[MFuture]:
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
        max_submitted_tasks = max_submitted_tasks or 500
        chunksize = chunksize or 100

        # before running, make sure cancel signal is False
        self.cancel_signal = False

        try:
            fargs = fargs or ()
            fkwargs = fkwargs or {}
            chunk = []
            for item in iterable:
                # abort if execution is cancelled
                if self.cancel_signal:  # pragma: no cover
                    logger.debug("executor cancelled")
                    return

                # skip task submission if option is activated
                if item_skip_bool:
                    item, skip, skip_info = item
                    if skip:
                        yield MFuture.skip(result=item, skip_info=skip_info)
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
        if chunk:
            logger.debug("submit chunk of %s items to cluster", len(chunk))
            futures = self._executor.map(partial(func, *fargs, **fkwargs), chunk)
            self._ac_iterator.update(futures)
            self._submitted += len(futures)

    def _yield_from_batch(self, batch):
        for future, result in batch:
            self._submitted -= 1
            if self.cancel_signal:  # pragma: no cover
                logger.debug("executor cancelled")
                raise JobCancelledError()
            yield self._finished_future(future, result, _dask=True)

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
