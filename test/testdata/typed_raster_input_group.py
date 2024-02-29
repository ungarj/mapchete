"""Example process file."""

import numpy.ma as ma

from mapchete import RasterInputGroup


def execute(
    rasters: RasterInputGroup,
) -> ma.MaskedArray:
    """User defined process."""
    for raster in rasters:
        if raster.is_empty():
            return "empty"

        data = raster.read(resampling="bilinear")

    return data
