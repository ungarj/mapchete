from mapchete.executor.base import ExecutorBase
from mapchete.executor.concurrent_futures import (
    MULTIPROCESSING_DEFAULT_START_METHOD,
    ConcurrentFuturesExecutor,
)
from mapchete.executor.dask import DaskExecutor
from mapchete.executor.future import MFuture
from mapchete.executor.sequential import SequentialExecutor

__all__ = ["MULTIPROCESSING_DEFAULT_START_METHOD", "MFuture"]


class Executor:
    """
    Executor factory for dask and concurrent.futures executor
    """

    def __new__(cls, *args, concurrency=None, **kwargs) -> ExecutorBase:
        if concurrency == "dask":
            return DaskExecutor(*args, **kwargs)

        elif concurrency is None:
            return SequentialExecutor(*args, **kwargs)

        elif concurrency in ["processes", "threads"]:
            return ConcurrentFuturesExecutor(*args, concurrency=concurrency, **kwargs)

        else:  # pragma: no cover
            raise ValueError(
                f"concurrency must be one of None, 'processes', 'threads' or 'dask', not {concurrency}"
            )
