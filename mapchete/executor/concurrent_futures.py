import concurrent.futures
from itertools import islice
import logging
import multiprocessing
import os
import sys
import warnings
from concurrent.futures._base import CancelledError
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    Iterator,
    Optional,
    Tuple,
    cast,
)

from mapchete.executor.base import ExecutorBase
from mapchete.executor.future import FutureProtocol, MFuture
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

    def __str__(self) -> str:
        return f"<ConcurrentFuturesExecutor max_workers={self.max_workers}, cls={self._executor_cls}>"

    def as_completed(
        self,
        func: Callable,
        iterable: Iterable,
        fargs: Optional[Tuple] = None,
        fkwargs: Optional[Dict[str, Any]] = None,
        max_submitted_tasks: int = 500,
        item_skip_bool: bool = False,
        **__,
    ) -> Generator[MFuture, None, None]:
        """Submit tasks to executor and start yielding finished futures."""

        # before running, make sure cancel signal is False
        self.cancel_signal = False
        fargs = fargs or ()
        fkwargs = fkwargs or {}

        try:
            # create an iterator
            iter_todo = iter(iterable)
            # submit first x tasks
            futures = set()

            logger.debug("submitting tasks to executor")
            with Timer() as duration:
                for item in islice(iter_todo, max_submitted_tasks):
                    if self.cancel_signal:  # pragma: no cover
                        logger.debug("executor cancelled")
                        return

                    # skip task submission if option is activated
                    if item_skip_bool:
                        item, skip, skip_info = item
                        if skip:
                            yield MFuture.skip(skip_info=skip_info, result=item)
                            continue
                    future = self._submit(func, item, fargs, fkwargs)
                    futures.add(future)
                    self.running_futures.add(future)
            logger.debug("first %s tasks submitted in %s", len(futures), duration)

            while futures and not self.cancel_signal:
                logger.debug("waiting for %s futures ...", len(futures))
                done, _ = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )
                logger.debug("%s future(s) done", len(done))
                for future in done:
                    yield self._finished_future(cast(FutureProtocol, future))
                    # we don't need this future anymore
                    futures.remove(future)
                    # immediately submit next task from iterator
                    try:
                        new_future = self._submit(func, next(iter_todo), fargs, fkwargs)
                        futures.add(new_future)
                        self.running_futures.add(new_future)
                    except StopIteration:
                        # nothing left to submit
                        pass

            if self.cancel_signal:  # pragma: no cover
                logger.debug("executor cancelled")
                return

        except CancelledError:  # pragma: no cover
            return
        finally:
            # reset so futures won't linger here for next call
            self.running_futures = set()
            self.finished_futures = set()

    def map(self, func, iterable, fargs=None, fkwargs=None) -> Iterable[Any]:
        return [
            result.output
            for result in map(
                self.func_partial(func, fargs=fargs, fkwargs=fkwargs), iterable
            )
        ]

    def _wait(self):
        concurrent.futures.wait(self.running_futures)

    def _as_completed(self, futures) -> Iterator[concurrent.futures.Future]:
        """Yield finished tasks."""
        for future in concurrent.futures.as_completed(futures):
            yield future

    def _submit(
        self, func: Callable, item: Any, fargs: tuple, fkwargs: dict
    ) -> concurrent.futures.Future:
        future = self._executor.submit(
            self.func_partial(func, fargs=fargs, fkwargs=fkwargs), item
        )
        self.running_futures.add(future)
        future.add_done_callback(self._add_to_finished)
        return future
