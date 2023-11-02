from typing import Union

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

    Will move into the mapchete core package.
    """

    def __new__(
        cls, *args, concurrency=None, **kwargs
    ) -> Union[ConcurrentFuturesExecutor, DaskExecutor, SequentialExecutor]:
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
