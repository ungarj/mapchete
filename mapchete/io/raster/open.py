from contextlib import contextmanager
from typing import Generator, Union

from rasterio.io import DatasetReader, DatasetWriter, BufferedDatasetWriter

from mapchete.io.raster.read import rasterio_read
from mapchete.io.raster.write import (
    rasterio_write,
)
from mapchete.path import MPath, MPathLike


@contextmanager
def rasterio_open(
    path: MPathLike, mode: str = "r", **kwargs
) -> Generator[
    Union[DatasetReader, DatasetWriter, BufferedDatasetWriter],
    None,
    None,
]:
    """Call rasterio.open but set environment correctly and return custom writer if needed."""
    path = MPath.from_inp(path)

    if "w" in mode:
        with rasterio_write(path, mode=mode, **kwargs) as dst:
            yield dst

    else:
        with rasterio_read(path, mode=mode, **kwargs) as src:
            yield src
