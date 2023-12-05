from typing import Optional

from mapchete.enums import Concurrency
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

    def __new__(
        cls, *args, concurrency: Optional[Concurrency] = None, **kwargs
    ) -> ExecutorBase:
        concurrency = (
            Concurrency.dask
            if kwargs.get("dask_scheduler") or kwargs.get("dask_client")
            else concurrency
        )

        if concurrency == Concurrency.dask:
            return DaskExecutor(*args, **kwargs)

        elif concurrency in [None, Concurrency.none]:
            return SequentialExecutor(*args, **kwargs)

        elif concurrency in [Concurrency.processes, Concurrency.threads]:
            return ConcurrentFuturesExecutor(*args, concurrency=concurrency, **kwargs)

        else:  # pragma: no cover
            raise ValueError(
                f"concurrency must be one of None, 'processes', 'threads' or 'dask', not {concurrency}"
            )
