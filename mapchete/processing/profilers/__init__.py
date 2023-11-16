from mapchete.executor.types import Profiler
from mapchete.processing.profilers.memory import measure_memory
from mapchete.processing.profilers.requests import measure_requests
from mapchete.processing.profilers.time import measure_time

preconfigured_profilers = [
    Profiler(name="time", decorator=measure_time),
    # NOTE: the order of requests and memory profilers is important as otherwise
    # it will cause an error
    Profiler(name="requests", decorator=measure_requests),
    Profiler(name="memory", decorator=measure_memory),
]


def pretty_bytes(count: float, round_value: int = 2) -> str:
    """Return human readable bytes."""
    count = float(count)

    for measurement in ["bytes", "KiB", "MiB", "GiB", "TiB"]:
        out = f"{round(count, round_value)} {measurement}"
        if count < 1024.0:
            break
        count /= 1024.0

    return out
