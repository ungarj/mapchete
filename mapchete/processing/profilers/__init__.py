from mapchete.executor.types import Profiler
from mapchete.processing.profilers.memory import MemoryTracker
from mapchete.processing.profilers.time import Timer

preconfigured_profilers = [
    Profiler(name="time", ctx=Timer),
    Profiler(name="memory", ctx=MemoryTracker),
]
