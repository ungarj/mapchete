import logging

from mapchete._core import (open, count_tiles, Mapchete, MapcheteProcess, Timer)


__all__ = ['open', 'count_tiles', 'Mapchete', 'MapcheteProcess', 'Timer']


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


__version__ = "0.28"
