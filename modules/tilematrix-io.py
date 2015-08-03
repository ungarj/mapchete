#!/usr/bin/env python

import rasterio
from tilematrix import *

def read_raster_window(input_file,
    tilematrix,
    tileindex,
    pixelbuffer=None,
    tilify=True):

    col, row, zoom = tileindex

    assert (isinstance(tilematrix, TileMatrix) or
        isinstance(tilematrix, MetaTileMatrix))

    # read source metadata
    with rasterio.open(input_file) as source:
        tl = [source.bounds.left, source.bounds.top]
        tr = [source.bounds.right, source.bounds.top]
        br = [source.bounds.right, source.bounds.bottom]
        bl = [source.bounds.left, source.bounds.bottom]
        source_envelope = Polygon([tl, tr, br, bl])
        source_crs = source.crs
        source_affine = source.affine
        source_meta = source.meta
        source_shape = source.shape
        source_nodata = int(source.nodatavals[0])

    # compute target metadata


    # compute target window

    # open with rasterio

    # if tilify, reproject/resample

    # return source metadata and data (numpy)


def read_vector_window(input_file,
    tilematrix,
    tileindex,
    pixelbuffer=None,
    tilify=True):

    col, row, zoom = tileindex

    assert (isinstance(tilematrix, TileMatrix) or
        isinstance(tilematrix, MetaTileMatrix))

    # read source metadata

    # compute target metadata

    # compute target window

    # open with fiona

    # if tilify, reproject/resample

    # return source metadata and data (shapely)