#!/usr/bin/env python
"""
Raster data read and write functions.
"""

import os
import rasterio
from rasterio.warp import transform_bounds, reproject
import numpy as np
import numpy.ma as ma
from copy import deepcopy

from .io_funcs import RESAMPLING_METHODS
from .numpy_io import write_numpy

def read_raster_window(
    input_file,
    tile,
    indexes=None,
    pixelbuffer=0,
    resampling="nearest"
    ):
    """
    Generates numpy arrays from input raster.
    """
    try:
        assert os.path.isfile(input_file)
    except:
        raise IOError("input file not found %s" % input_file)
    with rasterio.open(input_file, "r") as src:

        if indexes:
            if isinstance(indexes, list):
                band_indexes = indexes
            else:
                band_indexes = [indexes]
        else:
            band_indexes = src.indexes

        if tile.crs == src.crs:
            (src_left, src_bottom, src_right, src_top) = tile.bounds(
                pixelbuffer=pixelbuffer
                )
        else:
            # Reproject tile bounds to source file SRS.
            src_left, src_bottom, src_right, src_top = transform_bounds(
                tile.crs,
                src.crs,
                *tile.bounds(pixelbuffer=pixelbuffer),
                densify_pts=21
                )
            # TODO: find better fix to avoid invalid bounds values
            if float('Inf') in (src_left, src_bottom, src_right, src_top):
                src_left, src_bottom, src_right, src_top = transform_bounds(
                    tile.crs,
                    src.crs,
                    *tile.bounds(pixelbuffer=0),
                    densify_pts=21
                    )

        nodataval = src.nodata
        # Quick fix because None nodata is not allowed.
        if not nodataval:
            nodataval = 0
        minrow, mincol = src.index(src_left, src_top)
        maxrow, maxcol = src.index(src_right, src_bottom)

        # Calculate new Affine object for read window.
        window = (minrow, maxrow), (mincol, maxcol)
        window_vector_affine = src.affine.translation(
            mincol,
            minrow
            )
        window_affine = src.affine * window_vector_affine
        # Finally read data per band and store it in tuple.
        bands = (
            src.read(index, window=window, masked=True, boundless=True)
            for index in band_indexes
            )
        for index in band_indexes:
            dst_band = ma.zeros(
                (tile.shape(pixelbuffer=pixelbuffer)),
                src.dtypes[index-1]
            )
            dst_band[:] = nodataval
            reproject(
                next(bands),
                dst_band,
                src_transform=window_affine,
                src_crs=src.crs,
                src_nodata=nodataval,
                dst_transform=tile.affine(pixelbuffer=pixelbuffer),
                dst_crs=tile.crs,
                dst_nodata=nodataval,
                resampling=RESAMPLING_METHODS[resampling]
            )
            dst_band = ma.masked_equal(dst_band, nodataval)
            dst_band = ma.masked_array(
                dst_band,
                mask=ma.fix_invalid(dst_band, fill_value=0).mask
            )
            dst_band.harden_mask()
            yield dst_band

def write_raster(
    process,
    metadata,
    bands,
    pixelbuffer=0
    ):
    """
    Function to write arrays to either a NumPy dump or a raster file.
    """
    try:
        assert isinstance(bands, tuple) or isinstance(bands, np.ndarray)
    except:
        raise TypeError(
            "output bands must be stored in a tuple or a numpy array."
        )

    try:
        for band in bands:
            assert (
                isinstance(
                    band,
                    np.ndarray
                ) or isinstance(
                    band,
                    ma.MaskedArray
                )
            )
    except:
        raise TypeError(
            "output bands must be numpy ndarrays, not %s" % type(band)
            )

    process.tile.prepare_paths()

    if process.output.format == "NumPy":
        try:
            write_numpy(
                process.tile,
                metadata,
                bands,
                pixelbuffer=pixelbuffer
            )
        except:
            raise
    else:
        try:
            for band in bands:
                assert band.ndim == 2
        except:
            raise TypeError(
                "output bands must be 2-dimensional, not %s" % band.ndim
                )
        try:
            write_raster_window(
                process.tile.path,
                process.tile,
                bands,
                pixelbuffer=pixelbuffer
            )
        except:
            raise

def write_raster_window(
    output_file,
    tile,
    bands,
    pixelbuffer=0):
    """
    Writes numpy array into a TilePyramid tile.
    """
    try:
        assert pixelbuffer >= 0
    except:
        raise ValueError("pixelbuffer must be 0 or greater")

    try:
        assert isinstance(pixelbuffer, int)
    except:
        raise ValueError("pixelbuffer must be an integer")

    dst_width, dst_height = tile.shape(pixelbuffer)
    dst_affine = tile.affine(pixelbuffer)

    # determine pixelbuffer from shape and determine pixel window
    src_pixelbuffer = (bands[0].shape[0] - tile.width) / 2
    px_top, px_left = src_pixelbuffer, src_pixelbuffer
    other_bound = src_pixelbuffer + tile.width
    px_bottom, px_right = other_bound, other_bound

    dst_bands = []

    if tile.output.format == "PNG_hillshade":
        zeros = np.zeros(bands[0][px_top:px_bottom, px_left:px_right].shape)
        for band in range(1, 4):
            band = np.clip(band, 0, 255)
            dst_bands.append(zeros)

    for band in bands:
        dst_bands.append(band[px_top:px_bottom, px_left:px_right])

    bandcount = tile.output.bands

    if tile.output.format == "PNG":
        for band in bands:
            band = np.clip(band, 0, 255)
        if tile.output.nodataval:
            nodata_alpha = np.zeros(bands[0].shape)
            nodata_alpha[:] = 255
            nodata_alpha[bands[0].mask] = 0
            # just add alpha band if there is probably no alpha band yet
            if len(bands) not in [2, 4]:
                dst_bands.append(nodata_alpha[px_top:px_bottom, px_left:px_right])
            bandcount += 1

    dst_metadata = deepcopy(tile.output.profile)
    dst_metadata.pop("transform", None)
    dst_metadata.update(
        crs=tile.crs['init'],
        width=dst_width,
        height=dst_height,
        affine=dst_affine,
        driver=tile.output.format
    )

    if tile.output.format in ("PNG", "PNG_hillshade"):
        dst_metadata.update(
            dtype='uint8',
            count=bandcount,
            driver="PNG"
        )
    assert len(dst_bands) == dst_metadata["count"]
    with rasterio.open(output_file, 'w', **dst_metadata) as dst:
        for band, data in enumerate(dst_bands):
            data = np.ma.filled(data, dst_metadata["nodata"])
            dst.write(
                data.astype(dst_metadata["dtype"]),
                (band+1)
            )
