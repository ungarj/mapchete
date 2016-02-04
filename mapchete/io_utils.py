#!/usr/bin/env python

import mapchete
from tilematrix import *
from rasterio.warp import RESAMPLING

def read_raster(
    process,
    input_file,
    pixelbuffer=0,
    resampling=RESAMPLING.nearest
    ):
    """
    This is a wrapper around the read_raster_window function of tilematrix.
    Tilematrix itself uses rasterio to read rasterdata.
    This function returns a tuple of metadata and a numpy array containing the
    raster data clipped and resampled to the input tile.
    """
    # print process.tile
    # print process.tile_pyramid
    # print process.tile_pyramid.projection
    if input_file:
        metadata, data = read_raster_window(
            input_file,
            process.tile_pyramid,
            process.tile,
            pixelbuffer=pixelbuffer,
            resampling=resampling
            )
    else:
        metadata = None
        data = None

    return metadata, data
