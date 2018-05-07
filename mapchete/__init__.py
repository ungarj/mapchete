import logging

from mapchete._core import open, count_tiles, Mapchete, MapcheteProcess

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# suppress rasterio logging
logging.getLogger("rasterio").setLevel(logging.ERROR)


__version__ = "0.21"
