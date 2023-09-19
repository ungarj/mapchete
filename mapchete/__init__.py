import logging

from mapchete._core import Mapchete, open
from mapchete._executor import Executor, FakeFuture, SkippedFuture
from mapchete._processing import Job, ProcessInfo
from mapchete._timer import Timer
from mapchete._user_process import MapcheteProcess
from mapchete.tile import count_tiles

__all__ = [
    "open",
    "count_tiles",
    "Mapchete",
    "MapcheteProcess",
    "ProcessInfo",
    "Timer",
    "Executor",
    "FakeFuture",
    "SkippedFuture",
    "Job",
]
__version__ = "2023.9.1"

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
