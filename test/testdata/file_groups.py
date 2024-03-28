"""Example process file."""

import numpy.ma as ma

from mapchete import RasterInputGroup


def execute(
    group1: RasterInputGroup, group2: RasterInputGroup, nested_group: RasterInputGroup
) -> ma.MaskedArray:
    """User defined process."""

    # read and get mean of group1
    group1 = ma.mean(ma.stack([raster_input.read() for raster_input in group1]), axis=0)

    # read and get mean of group1
    group2 = ma.mean(ma.stack([raster_input.read() for raster_input in group2]), axis=0)

    # TODO nested_group

    return ma.mean(ma.stack([group1, group2]), axis=0)
