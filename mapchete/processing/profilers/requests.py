import logging
from dataclasses import dataclass
from typing import Any, Callable, Tuple, Union

logger = logging.getLogger(__name__)


@dataclass
class MeasuredRequests:
    head_count: int = 0
    get_count: int = 0
    get_bytes: int = 0


def measure_requests(add_to_return: bool = True) -> Callable:
    """Request counting decorator."""

    def wrapper(func: Callable) -> Callable:
        """Wrap a function."""

        def wrapped_f(*args, **kwargs) -> Union[Any, Tuple[Any, MeasuredRequests]]:
            try:
                from tilebench import profile
            except ImportError:  # pragma: no cover
                raise ImportError(
                    "please install tilebench if you want to use this feature."
                )

            @profile(add_to_return=True, quiet=True)
            def _decorated(func, fargs, fkwargs) -> Any:
                return func(*fargs, **fkwargs)

            retval, raw_results = _decorated(func, args, kwargs)
            results = MeasuredRequests(
                head_count=raw_results["HEAD"]["count"],
                get_count=raw_results["GET"]["count"],
                get_bytes=raw_results["GET"]["bytes"],
            )

            if add_to_return:
                return retval, results

            logger.info(
                "function %s caused %s HEAD requests, %s GET requests and retreived %sMB of data",
                func,
                results.head_count,
                results.get_count,
                round(results.get_bytes / 1024 / 1024, 2),
            )
            return retval

        return wrapped_f

    return wrapper
