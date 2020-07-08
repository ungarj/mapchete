import logging

from mapchete._core import open, Mapchete
from mapchete._processing import MapcheteProcess
from mapchete.tile import count_tiles
from mapchete._timer import Timer


__all__ = ['open', 'count_tiles', 'Mapchete', 'MapcheteProcess', 'Timer']
__version__ = "0.34"


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
