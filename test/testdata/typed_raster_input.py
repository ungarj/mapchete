"""Example process file."""

import numpy.ma as ma

from mapchete import MapcheteNodataTile, RasterInput


def execute(
    raster: RasterInput,
) -> ma.MaskedArray:
    """User defined process."""
    # Reading and writing data works like this:
    if raster.is_empty():
        # This assures a transparent tile instead of a pink error tile
        # is returned when using mapchete serve.
        raise MapcheteNodataTile

    data = raster.read(resampling="bilinear")
    return data
