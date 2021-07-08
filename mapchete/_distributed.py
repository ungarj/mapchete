from functools import partial
from itertools import chain
import logging
import multiprocessing
import os


logger = logging.getLogger(__name__)


################################################################################
# wrappers helping to abstract local multiprocessing or distributed processing #
################################################################################
class Executor:
    def __new__(self, *args, distributed=False, **kwargs):
        distributed = distributed or kwargs["dask_scheduler"] is not None
        if distributed:
            try:
                return DaskExecutor(*args, **kwargs)
            except ImportError as e:  # pragma: no cover
                raise ImportError(
                    f"this feature requires the mapchete[dask] extra: {e}"
                )
        else:
            return MultiprocessingExecutor(*args, **kwargs)


class DaskExecutor:
    """Execute tasks using dask cluster."""

    def __init__(
        self,
        *args,
        address=None,
        dask_scheduler=None,
        max_workers=None,
        **kwargs,
    ):
        from dask.distributed import LocalCluster

        local_cluster_kwargs = dict(
            n_workers=max_workers or os.cpu_count(), threads_per_worker=1
        )
        self.dask_scheduler = dask_scheduler or LocalCluster(**local_cluster_kwargs)
        logger.debug(f"init DaskExecutor with cluster at {self.dask_scheduler}")

    def as_completed(
        self, func=None, iterable=None, fargs=None, fkwargs=None, chunksize=1
    ):
        from dask.distributed import as_completed

        fargs = fargs or []
        fkwargs = fkwargs or {}
        for finished_task in as_completed(
            (
                self.client.submit(_exception_wrapper, func, fargs, fkwargs, i)
                for i in iterable
            )
        ):
            yield finished_task.result()

    def __enter__(self):
        """Enter context manager."""
        from dask.distributed import Client

        self.client = Client(address=self.dask_scheduler)
        logger.debug(f"client {self.client} initialized")
        return self

    def __exit__(self, *args):
        """Exit context manager."""
        logger.debug(f"exit client {self.client}...")
        self.client.__exit__(*args)


class MultiprocessingExecutor:
    """Wrapper class to be used with multiprocessing or billiard."""

    def __init__(
        self,
        *args,
        start_method="fork",
        max_workers=None,
        multiprocessing_module=multiprocessing,
        **kwargs,
    ):
        """Set attributes."""
        self.start_method = start_method
        self.max_workers = max_workers or os.cpu_count()
        self.multiprocessing_module = multiprocessing_module
        if self.max_workers != 1:
            logger.debug(
                "init %s MultiprocessingExecutor with start_method %s and %s workers",
                self.multiprocessing_module,
                self.start_method,
                self.max_workers,
            )
            self._pool = self.multiprocessing_module.get_context(
                self.start_method
            ).Pool(self.max_workers)

    def as_completed(
        self, func=None, iterable=None, fargs=None, fkwargs=None, chunksize=1
    ):
        """Yield finished tasks."""
        fargs = fargs or []
        fkwargs = fkwargs or {}
        if self.max_workers == 1:
            for i in iterable:
                yield _exception_wrapper(func, fargs, fkwargs, i)
        else:
            logger.debug(
                "open multiprocessing.Pool and %s %s workers",
                self.start_method,
                self.max_workers,
            )
            for finished_task in self._pool.imap_unordered(
                partial(_exception_wrapper, func, fargs, fkwargs),
                iterable,
                chunksize=chunksize or 1,
            ):
                yield finished_task

    def __enter__(self):
        """Enter context manager."""
        if self.max_workers != 1:
            self._pool.__enter__()
        return self

    def __exit__(self, *args):
        """Exit context manager."""
        if self.max_workers != 1:
            logger.debug("closing %s and workers", self._pool)
            self._pool.__exit__(*args)
            logger.debug("%s closed", self._pool)


class FinishedTask:
    """Wrapper class to encapsulate exceptions."""

    def __init__(self, func, fargs=None, fkwargs=None):
        """Set attributes."""
        fargs = fargs or []
        fkwargs = fkwargs or {}
        try:
            self._result, self._exception = func(*fargs, **fkwargs), None
        except Exception as e:  # pragma: no cover
            self._result, self._exception = None, e

    def result(self):
        """Return task result."""
        if self._exception:
            logger.exception(self._exception)
            raise self._exception
        else:
            return self._result

    def exception(self):
        """Raise task exception if any."""
        return self._exception

    def __repr__(self):
        """Return string representation."""
        return "FinishedTask(result=%s, exception=%s)" % (self._result, self._exception)


def _exception_wrapper(func, fargs, fkwargs, i):
    """Wrap function around FinishedTask object."""
    return FinishedTask(func, list(chain([i], fargs)), fkwargs)
