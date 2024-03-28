import logging
import os
from functools import cached_property
from sys import getsizeof
from typing import Any, Callable, Iterable, Iterator, List, Optional, Union

from dask.delayed import Delayed, DelayedLeaf
from dask.distributed import Client, LocalCluster, as_completed, wait

from mapchete.errors import JobCancelledError
from mapchete.executor.base import ExecutorBase
from mapchete.executor.future import MFuture
from mapchete.executor.types import Result
from mapchete.pretty import pretty_bytes

logger = logging.getLogger(__name__)


class DaskExecutor(ExecutorBase):
    """Execute tasks using dask cluster."""

    def __init__(
        self,
        *args,
        dask_scheduler: Optional[str] = None,
        dask_client: Optional[Client] = None,
        max_workers: int = os.cpu_count(),
        **kwargs,
    ):
        self.cancel_signal = False
        self._executor_client = dask_client
        self._local_cluster = None
        if self._executor_client:  # pragma: no cover
            logger.debug("using existing dask client: %s", dask_client)
        else:
            local_cluster_kwargs = dict(n_workers=max_workers, threads_per_worker=1)
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

    def __str__(self) -> str:
        return f"<DaskExecutor dashboard_link={self._executor.dashboard_link}>"

    def map(
        self,
        func: Callable,
        iterable: Iterable,
        fargs: Optional[tuple] = None,
        fkwargs: Optional[dict] = None,
    ) -> List[Any]:
        fargs = fargs or []
        fkwargs = fkwargs or {}

        def _extract_result(future):
            result = future.result()
            if isinstance(result, Result):
                return result.output
            return result  # pragma: no cover

        return [
            _extract_result(f)
            for f in self._executor.map(
                self.func_partial(func, *fargs, **fkwargs), iterable
            )
        ]

    def _wait(self):
        wait(self.running_futures)

    def _as_completed(self, *args, **kwargs) -> Iterator[MFuture]:  # pragma: no cover
        return

    def as_completed(
        self,
        func: Callable,
        iterable: Iterable,
        fargs: Optional[tuple] = None,
        fkwargs: Optional[dict] = None,
        max_submitted_tasks: int = 500,
        item_skip_bool: bool = False,
        chunksize: int = 100,
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

    def compute_task_graph(
        self,
        dask_collection: List[Union[Delayed, DelayedLeaf]],
        with_results: bool = False,
        raise_errors: bool = False,
    ) -> Iterator[MFuture]:
        # send to scheduler
        logger.debug("task graph has %s", pretty_bytes(getsizeof(dask_collection)))

        logger.debug(
            "sending %s tasks to cluster and wait for them to finish...",
            len(dask_collection),
        )
        for batch in as_completed(
            self._executor.compute(dask_collection, optimize_graph=True, traverse=True),
            with_results=with_results,
            raise_errors=raise_errors,
            loop=self._executor.loop,
        ).batches():
            for item in batch:
                if with_results:  # pragma: no cover
                    future, result = item
                else:
                    future, result = item, None
                if self.cancel_signal:  # pragma: no cover
                    logger.debug("executor cancelled")
                    raise JobCancelledError()
                yield self._finished_future(future, result, _dask=True)

    def _submit_chunk(
        self,
        chunk: List[Any],
        func: Callable,
        fargs: Optional[tuple] = None,
        fkwargs: Optional[dict] = None,
    ) -> None:
        if chunk:
            logger.debug("submit chunk of %s items to cluster", len(chunk))
            futures = self._executor.map(
                self.func_partial(func, fargs=fargs, fkwargs=fkwargs), chunk
            )
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
