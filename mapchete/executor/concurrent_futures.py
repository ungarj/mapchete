from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    Future,
    wait,
    FIRST_COMPLETED,
)
import logging
import multiprocessing
import os
import sys
import warnings
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    cast,
)

from mapchete.errors import JobCancelledError
from mapchete.executor.base import ExecutorBase
from mapchete.executor.future import FutureProtocol, MFuture
from mapchete.executor.types import Profiler
from mapchete.log import set_log_level
from mapchete.timer import Timer

logger = logging.getLogger(__name__)


MULTIPROCESSING_DEFAULT_START_METHOD = "spawn"


class ConcurrentFuturesExecutor(ExecutorBase):
    """Execute tasks using concurrent.futures."""

    def __init__(
        self,
        *args,
        max_workers=None,
        concurrency="processes",
        multiprocessing_start_method=None,
        profilers: Optional[List[Profiler]] = None,
        **kwargs,
    ):
        """Set attributes."""
        self.futures = set()
        self.profilers = profilers or []
        self._executor_args = ()
        self._executor_kwargs = dict()
        start_method = (
            multiprocessing_start_method or MULTIPROCESSING_DEFAULT_START_METHOD
        )
        self.max_workers = max_workers or kwargs.get("workers", os.cpu_count())
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
            self._executor_cls = ProcessPoolExecutor
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
            self._executor_cls = ThreadPoolExecutor
        else:  # pragma: no cover
            raise ValueError("concurrency must either be 'processes' or 'threads'")
        logger.debug(
            "init ConcurrentFuturesExecutor using %s with %s workers",
            concurrency,
            self.max_workers,
        )

    def __str__(self) -> str:
        return f"<ConcurrentFuturesExecutor max_workers={self.max_workers}, cls={self._executor_cls}>"

    def as_completed(
        self,
        func: Callable,
        iterable: Iterable,
        fargs: Optional[Tuple] = None,
        fkwargs: Optional[Dict[str, Any]] = None,
        item_skip_bool: bool = False,
        max_submitted_tasks: int = 100,
        **__,
    ) -> Generator[MFuture, None, None]:
        """Submit tasks to executor and start yielding finished futures."""
        fargs = fargs or ()
        fkwargs = fkwargs or {}

        # before running, make sure cancel signal is False
        self.cancel_signal = False

        # extract item skip tuples and make a generator
        item_skip_tuples = iter(
            ((item, skip_item, skip_info) for item, skip_item, skip_info in iterable)
            if item_skip_bool
            else ((item, False, None) for item in iterable)
        )
        futures = set()

        logger.debug("submitting tasks to executor")

        try:
            with Timer() as duration:
                for item, skip_item, skip_info in item_skip_tuples:
                    if self.cancel_signal:  # pragma: no cover
                        raise JobCancelledError("cancel signal caught")

                    # skip task submission if option is activated
                    if skip_item:
                        yield MFuture.skip(skip_info=skip_info, result=item)

                    # submit to executor
                    else:
                        futures.add(self._submit(func, item, fargs, fkwargs))

                        # don't submit any more until there are finished futures
                        if len(futures) == max_submitted_tasks:
                            break

            logger.debug("first %s tasks submitted in %s", len(futures), duration)

            while futures:
                if self.cancel_signal:  # pragma: no cover
                    raise JobCancelledError("cancel signal caught")

                logger.debug("waiting for %s futures ...", len(futures))
                done, _ = wait(futures, return_when=FIRST_COMPLETED)
                logger.debug("%s future(s) done", len(done))

                for future in done:
                    if self.cancel_signal:  # pragma: no cover
                        raise JobCancelledError("cancel signal caught")

                    yield self.to_mfuture(cast(FutureProtocol, future))

                    # we don't need this future anymore
                    futures.remove(future)

                    # immediately submit next task from iterator
                    try:
                        item, skip_item, skip_info = next(item_skip_tuples)

                        # skip task submission if option is activated
                        if skip_item:  # pragma: no cover
                            yield MFuture.skip(skip_info=skip_info, result=item)

                        # submit to executor
                        else:
                            futures.add(self._submit(func, item, fargs, fkwargs))

                    except StopIteration:
                        # nothing left to submit
                        pass

            if self.cancel_signal:  # pragma: no cover
                raise JobCancelledError("cancel signal caught")

        except JobCancelledError as exception:  # pragma: no cover
            logger.debug("%s", str(exception))

    def map(self, func, iterable, fargs=None, fkwargs=None) -> List[Any]:
        return [
            result.output  # type: ignore
            for result in self._executor.map(
                self.func_partial(func, fargs=fargs, fkwargs=fkwargs), iterable
            )
        ]

    def _submit(self, func: Callable, item: Any, fargs: tuple, fkwargs: dict) -> Future:
        future = self._executor.submit(
            self.func_partial(func, fargs=fargs, fkwargs=fkwargs), item
        )
        self.futures.add(future)  # type: ignore
        return future  # type: ignore

    def _wait(
        self,
        timeout: Optional[float] = None,
        return_when: Literal["FIRST_COMPLETED", "ALL_COMPLETED"] = "ALL_COMPLETED",
    ) -> None:
        wait(
            self.futures,  # type: ignore
            timeout=timeout,
            return_when=return_when,
        )
