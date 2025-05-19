import logging
import os
from functools import cached_property
from sys import getsizeof
from typing import (
    Any,
    Callable,
    Generator,
    Iterable,
    List,
    Literal,
    Optional,
    Set,
    Union,
    cast,
)

from dask.delayed import Delayed, DelayedLeaf
from dask.distributed import Client, LocalCluster, as_completed, wait, Future

from mapchete.errors import JobCancelledError
from mapchete.executor.base import ExecutorBase
from mapchete.executor.future import FutureProtocol, MFuture
from mapchete.executor.types import Profiler, Result
from mapchete.pretty import pretty_bytes
from mapchete.timer import Timer

logger = logging.getLogger(__name__)


class DaskExecutor(ExecutorBase):
    """Execute tasks using dask cluster."""

    def __init__(
        self,
        *_,
        dask_scheduler: Optional[str] = None,
        dask_client: Optional[Client] = None,
        max_workers: int = os.cpu_count() or 1,
        profilers: Optional[List[Profiler]] = None,
        **__,
    ):
        self.futures = set()
        self.profilers = profilers or []
        self._executor_args = ()
        self._executor_kwargs = dict()
        self.cancel_signal = False
        self._executor_client = dask_client
        self._local_cluster = None
        if self._executor_client:  # pragma: no cover
            logger.debug("using existing dask client: %s", dask_client)
        else:
            self._executor_cls = Client
            if dask_scheduler is None:
                logger.debug("start LocalCluster")
                self._local_cluster = LocalCluster(
                    n_workers=max_workers, threads_per_worker=1
                )
            self._executor_kwargs = dict(address=dask_scheduler or self._local_cluster)
            logger.debug(
                "starting dask.distributed.Client with kwargs %s", self._executor_kwargs
            )

    def __str__(self) -> str:
        return f"<DaskExecutor dashboard_link={self._executor.dashboard_link}>"

    def map(
        self,
        func: Callable,
        iterable: Iterable,
        fargs: Optional[tuple] = None,
        fkwargs: Optional[dict] = None,
    ) -> List[Any]:
        fargs = fargs or ()
        fkwargs = fkwargs or {}

        def _extract_result(future):
            result = future.result()
            if isinstance(result, Result):
                return result.output
            return result  # pragma: no cover

        return [
            _extract_result(f)
            for f in self._executor.map(
                self.func_partial(func, *fargs, **fkwargs),
                iterable,  # type: ignore
            )
        ]

    def as_completed(
        self,
        func: Callable,
        iterable: Iterable,
        fargs: Optional[tuple] = None,
        fkwargs: Optional[dict] = None,
        item_skip_bool: bool = False,
        chunksize: int = 100,
        max_submitted_tasks: int = 500,
        **__,
    ) -> Generator[MFuture, None, None]:
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
        # before running, make sure cancel signal is False
        self.cancel_signal = False

        # extract item skip tuples and make a generator
        item_skip_tuples = (
            ((item, skip_item, skip_info) for item, skip_item, skip_info in iterable)
            if item_skip_bool
            else ((item, False, None) for item in iterable)
        )

        try:
            # collect tasks to automatically submit them in chunks
            with TaskManager(
                executor=self,
                submit_chunksize=chunksize,
                func=func,
                fargs=fargs,
                fkwargs=fkwargs,
            ) as task_manager:
                # (1) submit first x tasks
                with Timer() as duration:
                    for item, skip_item, skip_info in item_skip_tuples:
                        self.raise_if_cancelled()

                        if skip_item:
                            yield MFuture.skip(skip_info=skip_info, result=item)

                        else:
                            # add processing item to chunk
                            task_manager.add_to_items(item)

                            # if enough tasks are scheduled to be submitted, quit loop
                            if task_manager.total_futures_count == max_submitted_tasks:
                                break

                    # submit remaining tasks until maximum tasks limit is reached
                    task_manager.submit_items()

                logger.debug(
                    "first %s tasks submitted in %s",
                    task_manager.remote_futures_count,
                    duration,
                )
                # TODO: remove this later
                assert (
                    task_manager.remote_futures_count
                    == task_manager.total_futures_count
                )

                # (2) now wait for the first tasks to finish until submitting the next ones
                while task_manager.remote_futures_count:
                    logger.debug(
                        "waiting for %s running futures ...",
                        task_manager.remote_futures_count,
                    )

                    for future in task_manager.finished_futures():
                        yield future

                        # for each finished task, schedule another one
                        try:
                            item, skip_item, skip_info = next(item_skip_tuples)
                            if skip_item:  # pragma: no cover
                                yield MFuture.skip(skip_info=skip_info, result=item)
                            else:
                                # add another processing item to chunk
                                task_manager.add_to_items(item)
                        except StopIteration:
                            pass

                    # submit remaining tasks
                    task_manager.submit_items()

                logger.debug(
                    "%s tasks submitted in total", task_manager.total_futures_count
                )

        except JobCancelledError as exception:
            logger.debug("%s", str(exception))

    def compute_task_graph(
        self,
        dask_collection: List[Union[Delayed, DelayedLeaf]],
        with_results: bool = False,
        raise_errors: bool = False,
    ) -> Generator[MFuture, None, None]:
        # send to scheduler
        with TaskManager(
            self, with_results=with_results, raise_errors=raise_errors
        ) as task_manager:
            task_manager.submit_graph(dask_collection=dask_collection)
            for future in task_manager.finished_futures():
                yield future

    def raise_if_cancelled(self):
        if self.cancel_signal:  # pragma: no cover
            raise JobCancelledError("cancel signal caught")

    @cached_property
    def _executor(self):  # type: ignore
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

    def _wait(
        self,
        timeout: Optional[float] = None,
        return_when: Literal["FIRST_COMPLETED", "ALL_COMPLETED"] = "ALL_COMPLETED",
    ) -> None:  # pragma: no cover
        wait(self.futures, timeout=timeout, return_when=return_when)


class TaskManager:
    executor: DaskExecutor
    submit_chunksize: int = 100
    func: Optional[Callable] = None
    fargs: Optional[tuple] = None
    fkwargs: Optional[dict] = None
    items: List[Any]

    def __init__(
        self,
        executor: DaskExecutor,
        submit_chunksize: int = 100,
        func: Optional[Callable] = None,
        fargs: Optional[tuple] = None,
        fkwargs: Optional[dict] = None,
        with_results: bool = True,
        raise_errors: bool = False,
    ):
        self.executor = executor
        self.submit_chunksize = submit_chunksize
        self.func = func
        self.fargs = fargs or ()
        self.fkwargs = fkwargs or {}
        self.items = []
        self.total_futures_count = 0
        self.remote_futures_count = 0
        self.with_results = with_results
        self.raise_errors = raise_errors

    def __len__(self) -> int:
        return len(self.items)

    def __enter__(self):
        """Enter context manager."""
        self.as_completed_iterator = as_completed(
            loop=self.executor._executor.loop,
            with_results=self.with_results,
            raise_errors=self.raise_errors,
        )
        return self

    def __exit__(self, *_):
        """Clean up."""
        self.as_completed_iterator.clear()
        self.executor.wait()

    def add_to_items(self, item: Any, submit: bool = False):
        self.executor.raise_if_cancelled()

        self.items.append(item)
        self.total_futures_count += 1
        if submit or len(self) == self.submit_chunksize:
            self.submit_items()

    def submit_items(self) -> Set[Future]:
        self.executor.raise_if_cancelled()

        if self.items:
            if self.func is None:  # pragma: no cover
                raise ValueError("func not provided")

            logger.debug("submit %s tasks to cluster", len(self))

            futures = set(
                self.executor._executor.map(
                    self.executor.func_partial(
                        self.func, fargs=self.fargs, fkwargs=self.fkwargs
                    ),
                    self.items,
                )
            )
            self.as_completed_iterator.update(futures)
            self.remote_futures_count += len(futures)
            self.items = []
            return futures

        return set()

    def submit_graph(
        self,
        dask_collection: List[Union[Delayed, DelayedLeaf]],
        optimize_graph: bool = True,
        traverse: bool = True,
    ):
        logger.debug("task graph has %s", pretty_bytes(getsizeof(dask_collection)))

        if dask_collection:
            logger.debug(
                "sending %s tasks to cluster and wait for them to finish...",
                len(dask_collection),
            )
            futures: List[Future] = self.executor._executor.compute(
                dask_collection, optimize_graph=optimize_graph, traverse=traverse
            )  # type: ignore
            self.executor.raise_if_cancelled()

            self.as_completed_iterator.update(futures)
            self.remote_futures_count += len(futures)
            self.total_futures_count += len(futures)

    def finished_futures(self) -> Generator[MFuture, None, None]:
        self.executor.raise_if_cancelled()

        for batch in self.as_completed_iterator.batches():
            logger.debug("%s future(s) done", len(batch))

            for future in batch:
                self.executor.raise_if_cancelled()

                if self.with_results:
                    future, result = future
                else:
                    result = None
                self.remote_futures_count -= 1

                yield self.executor.to_mfuture(
                    cast(FutureProtocol, future), result=result
                )
