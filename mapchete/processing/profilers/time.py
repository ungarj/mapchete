import logging
from dataclasses import dataclass
from typing import Any, Callable, Tuple, Union

from mapchete.timer import Timer

logger = logging.getLogger(__name__)


@dataclass
class MeasuredTime:
    start: int
    end: int
    elapsed: int


def measure_time(add_to_return: bool = True) -> Callable:
    """Time tracking decorator."""

    def wrapper(func: Callable) -> Callable:
        """Wrap a function."""

        def wrapped_f(*args, **kwargs) -> Union[Any, Tuple[Any, MeasuredTime]]:
            with Timer() as timed:
                retval = func(*args, **kwargs)

            result = MeasuredTime(
                elapsed=timed.elapsed, start=timed.start, end=timed.end
            )
            logger.info("function %s took %s", func, str(timed))
            if add_to_return:
                return (retval, result)

        return wrapped_f

    return wrapper
