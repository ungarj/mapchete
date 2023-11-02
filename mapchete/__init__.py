import logging

from mapchete._core import Mapchete, open
from mapchete._processing import Job, ProcessInfo
from mapchete._timer import Timer
from mapchete._user_process import MapcheteProcess
from mapchete.executor import Executor, MFuture
from mapchete.tile import count_tiles

__all__ = [
    "open",
    "count_tiles",
    "Mapchete",
    "MapcheteProcess",
    "ProcessInfo",
    "Timer",
    "Executor",
    "MFuture",
    "Job",
]
__version__ = "2023.10.0"

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
