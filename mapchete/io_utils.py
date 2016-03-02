#!/usr/bin/env python

import numpy as np

import mapchete
from tilematrix import *

def read_vector(
    process,
    input_file,
    pixelbuffer=0
    ):
    """
    This is a wrapper around the read_vector_window function of tilematrix.
    Tilematrix itself uses fiona to read vector data.
    This function returns a list of GeoJSON-like dictionaries containing the
    clipped vector data and attributes.
    """
    if input_file:
        features = read_vector_window(
            input_file,
            process.tile_pyramid,
            process.tile,
            pixelbuffer=pixelbuffer
        )
    else:
        features = None

    return features


def read_raster(
    process,
    input_file,
    pixelbuffer=0,
    resampling="nearest"
    ):
    """
    This is a wrapper around the read_raster_window function of tilematrix.
    Tilematrix itself uses rasterio to read raster data.
    This function returns a tuple of metadata and a numpy array containing the
    raster data clipped and resampled to the input tile.
    """

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

    # Return None if bands are empty.
    all_bands_empty = True
    for band_data in data:
        if not band_data.mask.all():
            all_bands_empty = False
    if all_bands_empty:
        metadata = None
        data = None

    return metadata, data


def write_raster(
    process,
    metadata,
    bands,
    pixelbuffer=0
    ):
    try:
        assert isinstance(bands, tuple)
    except:
        raise TypeError("output bands must be stored in a tuple.")

    try:
        for band in bands:
            assert (
                isinstance(
                    band,
                    np.ndarray
                ) or isinstance(
                    band,
                    np.ma.core.MaskedArray
                )
            )
    except:
        raise TypeError(
            "output bands must be numpy ndarrays, not %s" % type(band)
            )

    try:
        for band in bands:
            assert band.ndim == 2
    except:
        raise TypeError(
            "output bands must be 2-dimensional, not %s" % band.ndim
            )

    process.tile_pyramid.format.prepare(
        process.config["output_name"],
        process.tile
    )

    out_file = process.tile_pyramid.format.get_tile_name(
        process.config["output_name"],
        process.tile
    )

    try:
        write_raster_window(
            out_file,
            process.tile_pyramid,
            process.tile,
            metadata,
            bands,
            pixelbuffer=pixelbuffer
        )
    except:
        raise
