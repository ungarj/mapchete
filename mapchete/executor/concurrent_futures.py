import concurrent.futures
import logging
import multiprocessing
import os
import sys
import warnings
from concurrent.futures._base import CancelledError
from typing import Any, Callable, Iterator

from mapchete.executor.base import ExecutorBase
from mapchete.executor.future import MFuture
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
        func,
        iterable,
        fargs=None,
        fkwargs=None,
        max_submitted_tasks=500,
        item_skip_bool=False,
        **kwargs,
    ) -> Iterator[MFuture]:
        """Submit tasks to executor and start yielding finished futures."""

        # before running, make sure cancel signal is False
        self.cancel_signal = False

        try:
            fargs = fargs or ()
            fkwargs = fkwargs or {}
            logger.debug("submitting tasks to executor")
            i = 0
            with Timer() as timer:
                for i, item in enumerate(iterable, 1):
                    if self.cancel_signal:  # pragma: no cover
                        logger.debug("executor cancelled")
                        return

                    # skip task submission if option is activated
                    if item_skip_bool:
                        item, skip, skip_info = item
                        if skip:
                            yield MFuture.skip(skip_info=skip_info, result=item)
                            continue

                    # submit task to workers
                    self._submit(func, item, fargs, fkwargs)

                    # yield finished tasks if any
                    ready = self._ready()
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

            logger.debug(
                "%s tasks submitted in %s (%s still running)",
                i,
                timer,
                len(self.running_futures),
            )

            # yield remaining futures as they finish
            for future in self._as_completed(self.running_futures):
                yield self._finished_future(future)

        except CancelledError:  # pragma: no cover
            return
        finally:
            # reset so futures won't linger here for next call
            self.running_futures = set()
            self.finished_futures = set()

    def map(self, func, iterable, fargs=None, fkwargs=None) -> Iterator[Any]:
        fargs = fargs or []
        fkwargs = fkwargs or {}
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

    def _submit(self, func: Callable, item: Any, fargs: tuple, fkwargs: dict) -> None:
        future = self._executor.submit(
            self.func_partial(func, fargs=fargs, fkwargs=fkwargs), item
        )
        self.running_futures.add(future)
        future.add_done_callback(self._add_to_finished)
