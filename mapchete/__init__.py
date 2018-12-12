import logging

from mapchete._core import (open, count_tiles, Mapchete, MapcheteProcess, Timer)


__all__ = ['open', 'count_tiles', 'Mapchete', 'MapcheteProcess', 'Timer']


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# suppress spammy loggers
logging.getLogger("rasterio").setLevel(logging.ERROR)
logging.getLogger("fiona").setLevel(logging.ERROR)
logging.getLogger("shapely").setLevel(logging.ERROR)


__version__ = "0.27"
