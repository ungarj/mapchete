import logging

from mapchete._core import open, Mapchete
from mapchete._executor import Executor
from mapchete._processing import Job, MapcheteProcess, ProcessInfo
from mapchete.tile import count_tiles
from mapchete._timer import Timer


__all__ = ["open", "count_tiles", "Mapchete", "MapcheteProcess", "ProcessInfo", "Timer"]
__version__ = "2021.10.0"


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
