#!/usr/bin/env python

import numpy as np
import numpy.ma as ma
import os
from copy import deepcopy
from rasterio.warp import calculate_default_transform
from affine import Affine

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
    bands=None,
    resampling="nearest",
    return_empty_mask=False
    ):
    """
    This is a wrapper around the read_raster_window function of tilematrix.
    Tilematrix itself uses rasterio to read raster data.
    This function returns a tuple of metadata and a numpy array containing the
    raster data clipped and resampled to the input tile.
    """
    if input_file and os.path.isfile(input_file):
        metadata, data = read_raster_window(
            input_file,
            process.tile_pyramid,
            process.tile,
            bands=bands,
            pixelbuffer=pixelbuffer,
            resampling=resampling
            )
    else:
        metadata = None
        data = None

    if not return_empty_mask:
        # Return None if bands are empty.
        all_bands_empty = True
        for band_data in data:
            if not band_data.mask.all():
                all_bands_empty = False
        if all_bands_empty:
            metadata = None
            data = None
    return metadata, data


def read_pyramid(
    dst_tile,
    src_output_name,
    src_tile_pyramid,
    src_zoom=None,
    dst_tile_pyramid=None,
    dst_pixelbuffer=0,
    resampling="nearest"
    ):
    """
    This function reads from an existing tile pyramid.
    """
    zoom, row, col = dst_tile
    if not src_zoom:
        src_zoom = zoom
    if not dst_tile_pyramid:
        dst_tile_pyramid = src_tile_pyramid
    tile_bbox = dst_tile_pyramid.tile_bbox(*dst_tile, pixelbuffer=dst_pixelbuffer)
    src_tiles = src_tile_pyramid.tiles_from_geom(tile_bbox, src_zoom)
    rows = {}
    for src_zoom, src_row, src_col in sorted(src_tiles, key=lambda x:x[2]):
        rows.setdefault(src_row, []).append((src_zoom, src_row, src_col))
    rows_data = {}
    rows_metadata = ()
    temp = type('temp', (object,), {})()
    temp.tile_pyramid = TilePyramid("geodetic", tile_size=128)
    temp.tile = dst_tile

    for row, tiles in rows.iteritems():
        row_data = {}
        for band in range(dst_tile_pyramid.format.profile["count"]):
            row_data[band] = ()
        for tile in tiles:
            print tile
            filename = src_tile_pyramid.format.get_tile_name(
                src_output_name,
                tile
            )
            print "read"
            tile_metadata, tile_data = read_raster(
                temp,
                filename,
                return_empty_mask=True,
                bands = dst_tile_pyramid.format.profile["count"]
            )
            # print dst_tile_pyramid.format.profile["count"]
            if not tile_data:
                print "empty"
                tile_data = ()
                size = temp.tile_pyramid.tile_size
                for i in range(1, dst_tile_pyramid.format.profile["count"]+1):
                    zeros = np.zeros(
                        shape=((size, size)),
                        dtype=dst_tile_pyramid.format.profile["dtype"]
                    )
                    out_band = ma.masked_array(
                        zeros,
                        mask=True
                    )
                    tile_data += (out_band,)
                tile_metadata = None
            print len(tile_data)
            assert len(tile_data) == 3
            for band in tile_data:
                assert isinstance(band, np.ndarray)

                assert band.shape == (128, 128)
                # print "is tile data", (tile_data)
                print "all masked", (band.all() is np.ma.masked)
            # add tile bands to rows data
            for tile_band, tile_banddata in enumerate(tile_data):
                row_data[tile_band] += (tile_banddata, )
            assert len(row_data) == 3
        for band, banddata in row_data.iteritems():
            assert isinstance(banddata, tuple)
            assert len(banddata) == 2
        rows_data[row] = row_data

    rows_mosaic = {}
    for band in range(dst_tile_pyramid.format.profile["count"]):
        rows_mosaic[band] = ()
    for row, row_data in rows_data.iteritems():
        row_mosaic = {}
        for band, banddata in row_data.iteritems():
            assert len(banddata) == 2
            assert isinstance(banddata, tuple)
            row_mosaic[band] = np.hstack(banddata)
            assert row_mosaic[band].shape == (128, 256)
            rows_mosaic[band] += (row_mosaic[band], )

    mosaic = ()
    assert len(rows_mosaic) == 3
    for band, banddata in rows_mosaic.iteritems():
        assert isinstance(banddata, tuple)
        assert len(banddata) == 2
        band_mosaic = np.vstack(banddata)
        dst_shape = band_mosaic.shape
        mosaic += (band_mosaic, )

    assert isinstance(mosaic, tuple)
    assert len(mosaic) == 3
    for band in mosaic:
        assert isinstance(band, np.ndarray)
        print "mosaic masked", (band.all() is np.ma.masked)
        print "max", np.amax(band)
        assert band.shape == (256, 256)

    dst_left, dst_bottom, dst_right, dst_top = dst_tile_pyramid.tile_bounds(
        *dst_tile,
        pixelbuffer=dst_pixelbuffer
        )
    dst_width, dst_height = dst_shape


    # Create tile affine
    px_size = src_tile_pyramid.pixel_x_size(zoom)
    tile_geotransform = (dst_left, px_size, 0.0, dst_top, 0.0, -px_size)
    dst_affine = Affine.from_gdal(*tile_geotransform)

    dst_metadata = deepcopy(dst_tile_pyramid.format.profile)
    dst_metadata.pop("transform", None)
    dst_metadata["nodata"] = 255#dst_tile_pyramid.format.profile["nodata"]
    dst_metadata["crs"] = dst_tile_pyramid.crs['init']
    dst_metadata["width"] = dst_width
    dst_metadata["height"] = dst_height
    dst_metadata["affine"] = dst_affine
    dst_metadata["count"] = len(mosaic)
    dst_metadata["dtype"] = dst_tile_pyramid.format.profile["dtype"]

    return dst_metadata, mosaic


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
        process.config.output_name,
        process.tile
    )

    out_file = process.tile_pyramid.format.get_tile_name(
        process.config.output_name,
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
