from itertools import islice
import logging
import os
from functools import cached_property
from sys import getsizeof
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

from dask.delayed import Delayed, DelayedLeaf
from dask.distributed import Client, LocalCluster, as_completed, wait, Future

from mapchete.errors import JobCancelledError
from mapchete.executor.base import ExecutorBase
from mapchete.executor.future import FutureProtocol, MFuture
from mapchete.executor.types import Result
from mapchete.pretty import pretty_bytes
from mapchete.timer import Timer

logger = logging.getLogger(__name__)


class DaskExecutor(ExecutorBase):
    """Execute tasks using dask cluster."""

    _submit_queue: List[Tuple[Callable, Any]]
    _executor_args: Tuple
    _executor_kwargs: Dict[str, Any]

    def __init__(
        self,
        *args,
        dask_scheduler: Optional[str] = None,
        dask_client: Optional[Client] = None,
        max_workers: int = os.cpu_count() or 1,
        **kwargs,
    ):
        self.cancel_signal = False
        self._executor_client = dask_client
        self._local_cluster = None
        self._executor_args = ()
        self._executor_kwargs = dict()
        if self._executor_client:  # pragma: no cover
            logger.debug("using existing dask client: %s", dask_client)
        else:
            self._executor_cls = Client
            if dask_scheduler is None:
                self._local_cluster = LocalCluster(
                    n_workers=max_workers, threads_per_worker=1
                )
            self._executor_kwargs = dict(address=dask_scheduler or self._local_cluster)
            logger.debug(
                "starting dask.distributed.Client with kwargs %s", self._executor_kwargs
            )
        self._ac_iterator = as_completed(
            loop=self._executor.loop, with_results=True, raise_errors=False
        )
        self._submitted = 0
        _submit_queue = []
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
        max_submitted_tasks: int = 500,
        item_skip_bool: bool = False,
        chunksize: int = 100,
        **kwargs,
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
        # collect tasks to automatically submit them in chunks
        tasks_chunk = TaskChunk(
            executor=self,
            chunksize=chunksize,
            func=func,
            fargs=fargs or (),
            fkwargs=fkwargs or {},
        )
        try:
            # create an iterator
            iter_todo = iter(iterable)

            # submit first x tasks
            with Timer() as duration:
                for item in islice(iter_todo, max_submitted_tasks):
                    if self.cancel_signal:  # pragma: no cover
                        raise JobCancelledError("cancel signal caught")

                    # skip task submission if option is activated
                    if item_skip_bool:
                        item, skip, skip_info = item
                        if skip:
                            yield MFuture.skip(skip_info=skip_info, result=item)
                            continue
                    # add processing item to chunk
                    tasks_chunk.add(item)

                # submit remainder of tasks until maximum tasks limit is reached
                tasks_chunk.submit()

            logger.debug(
                "first %s tasks submitted in %s",
                len(self._ac_iterator.futures),
                duration,
            )

            # now wait for the first tasks to finish until submitting the next ones
            while not (self._ac_iterator.is_empty() or self.cancel_signal):
                logger.debug(
                    "waiting for %s running futures ...", len(self._ac_iterator.futures)
                )
                for batch in self._ac_iterator.batches():
                    logger.debug("%s future(s) done", len(batch))
                    for future, result in batch:
                        if self.cancel_signal:  # pragma: no cover
                            raise JobCancelledError("cancel signal caught")
                        finished_future = self._finished_future(
                            cast(FutureProtocol, future), result=result
                        )
                        yield finished_future

                        # for each finished task, schedule another one
                        try:
                            item = next(iter_todo)
                            if item_skip_bool:
                                item, skip, skip_info = item
                                if skip:
                                    yield MFuture.skip(skip_info=skip_info, result=item)
                                    continue

                            # add processing item to chunk
                            tasks_chunk.add(item)
                        except StopIteration:
                            pass

                if self.cancel_signal:  # pragma: no cover
                    raise JobCancelledError("cancel signal caught")

                # submit final tasks
                tasks_chunk.submit()

        except JobCancelledError as exception:  # pragma: no cover
            logger.debug("%s", str(exception))

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
    ) -> Set[Future]:
        if chunk:
            logger.debug("submit chunk of %s items to cluster", len(chunk))
            futures = set(
                self._executor.map(
                    self.func_partial(func, fargs=fargs, fkwargs=fkwargs), chunk
                )
            )
            self._ac_iterator.update(futures)
            self.futures.update(futures)  # type: ignore
            return futures
        return set()

    def _yield_from_batch(self, batch):
        for future, result in batch:
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

    def _wait(
        self,
        timeout: Optional[float] = None,
        return_when: Literal["FIRST_COMPLETED", "ALL_COMPLETED"] = "ALL_COMPLETED",
    ) -> None:
        wait(self.futures, timeout=timeout, return_when=return_when)


class TaskChunk:
    executor: DaskExecutor
    chunksize: int
    func: Callable
    fargs: Optional[tuple] = None
    fkwargs: Optional[dict] = None
    items: List[Any]

    def __init__(
        self,
        executor: DaskExecutor,
        chunksize: int,
        func: Callable,
        fargs: Optional[tuple] = None,
        fkwargs: Optional[dict] = None,
    ):
        self.executor = executor
        self.chunksize = chunksize
        self.func = func
        self.fargs = fargs or ()
        self.fkwargs = fkwargs or {}
        self.items = []

    def __len__(self) -> int:
        return len(self.items)

    def add(self, item: Any):
        self.items.append(item)
        if len(self) % self.chunksize == 0:
            logger.debug("chunk is full (%s), submit to cluster", len(self))
            self.submit()

    def submit(self):
        logger.debug("submit %s tasks to cluster", len(self))
        self.executor._submit_chunk(
            chunk=self.items, func=self.func, fargs=self.fargs, fkwargs=self.fkwargs
        )
        self.items = []
