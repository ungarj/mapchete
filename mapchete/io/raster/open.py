from contextlib import contextmanager

from mapchete.io.raster.read import rasterio_read
from mapchete.io.raster.write import rasterio_write
from mapchete.path import MPath


@contextmanager
def rasterio_open(path, mode="r", **kwargs):
    """Call rasterio.open but set environment correctly and return custom writer if needed."""
    path = MPath.from_inp(path)

    if "w" in mode:
        with rasterio_write(path, mode=mode, **kwargs) as dst:
            yield dst

    else:
        with rasterio_read(path, mode=mode, **kwargs) as src:
            yield src
