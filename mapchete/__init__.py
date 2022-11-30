import logging

from mapchete._core import open, Mapchete
from mapchete._executor import Executor, FakeFuture, SkippedFuture
from mapchete._processing import Job, ProcessInfo
from mapchete._user_process import MapcheteProcess
from mapchete.tile import count_tiles
from mapchete._timer import Timer


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
__version__ = "2022.11.2"

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
