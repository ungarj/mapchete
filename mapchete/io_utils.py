#!/usr/bin/env python

import mapchete
from tilematrix import *

def read_raster(process, input_file):
    """
    This is a wrapper around the read_raster_window function of tilematrix.
    Tilematrix itself uses rasterio to read rasterdata.
    This function returns a tuple of metadata and a numpy array containing the
    raster data clipped and resampled to the input tile.
    """
    # print process.tile
    # print process.tile_pyramid
    # print input_file
    # print process.tile_pyramid.projection
    metadata, data = read_raster_window(input_file,
        process.tile_pyramid,
        process.tile
        )
    return metadata
