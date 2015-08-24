#!/usr/bin/env python

import rasterio
from rasterio.warp import *
import numpy as np
import numpy.ma as ma
from copy import deepcopy

from tilematrix import *

def read_raster_window(input_file,
    tilematrix,
    tileindex,
    pixelbuffer=None,
    tilify=True):

    zoom, row, col = tileindex

    assert (isinstance(tilematrix, TileMatrix) or
        isinstance(tilematrix, MetaTileMatrix))

    # read source metadata
    source_envelope = raster_bbox(input_file, tilematrix)
    with rasterio.open(input_file) as source:
        source_crs = source.crs
        source_affine = source.affine
        source_meta = source.meta
        source_shape = source.shape
        source_dtype = source.dtypes[0]

        # Try to get NODATA value. Set to 0 if not available.
        try:
            source_nodata = int(source.nodatavals[0])
        except:
            source_nodata = 0

    # compute target metadata and initiate numpy array
    tile_geom = tilematrix.tile_bbox(zoom, row, col, pixelbuffer)

    try:
        assert tile_geom.intersects(source_envelope)

        left, bottom, right, top = tilematrix.tile_bounds(zoom, row, col,
            pixelbuffer)
        pixelsize = tilematrix.pixelsize(zoom)
        if pixelbuffer:
            destination_pixel = tilematrix.px_per_tile + (pixelbuffer * 2)
        else:
            destination_pixel = tilematrix.px_per_tile
        destination_shape = (destination_pixel, destination_pixel)
        destination_crs = tilematrix.crs
        width, height = destination_shape
        destination_data = np.zeros(destination_shape, dtype=source_dtype)


        # compute target window
        out_left, out_bottom, out_right, out_top = transform_bounds(
            source_crs, destination_crs, left, bottom, right, top, densify_pts=21)

        # compute target affine
        destination_affine = calculate_default_transform(
            source_crs,
            destination_crs,
            width,
            height,
            out_left,
            out_bottom,
            out_right,
            out_top,
            resolution=(pixelsize, pixelsize))[0]

        # open window with rasterio
        with rasterio.open(input_file) as source:
            minrow, mincol = source.index(out_left, out_top)
            maxrow, maxcol = source.index(out_right, out_bottom)
            window_offset_row = minrow
            window_offset_col = mincol
            minrow, minrow_offset = clean_pixel_coordinates(minrow, source_shape[0])
            maxrow, maxrow_offset = clean_pixel_coordinates(maxrow, source_shape[0])
            mincol, mincol_offset = clean_pixel_coordinates(mincol, source_shape[1])
            maxcol, maxcol_offset = clean_pixel_coordinates(maxcol, source_shape[1])
            rows = (minrow, maxrow)
            cols = (mincol, maxcol)

            window_data = source.read(1, window=(rows, cols))
            if minrow_offset:
                nullarray = np.empty((minrow_offset, window_data.shape[1]), dtype=source_dtype)
                nullarray[:] = source_nodata
                newarray = np.concatenate((nullarray, window_data), axis=0)
                window_data = newarray
            if maxrow_offset:
                nullarray = np.empty((maxrow_offset, window_data.shape[1]), dtype=source_dtype)
                nullarray[:] = source_nodata
                newarray = np.concatenate((window_data, nullarray), axis=0)
                window_data = newarray
            if mincol_offset:
                nullarray = np.empty((window_data.shape[0], mincol_offset), dtype=source_dtype)
                nullarray[:] = source_nodata
                newarray = np.concatenate((nullarray, window_data), axis=1)
                window_data = newarray
            if maxcol_offset:
                nullarray = np.empty((window_data.shape[0], maxcol_offset), dtype=source_dtype)
                nullarray[:] = source_nodata
                newarray = np.concatenate((window_data, nullarray), axis=1)
                window_data = newarray

            window_vector_affine = source_affine.translation(window_offset_col, window_offset_row)
            window_affine = source_affine * window_vector_affine

            window_meta = source_meta
            window_meta['transform'] = window_affine
            window_meta['height'] = window_data.shape[0]
            window_meta['width'] = window_data.shape[1]
            window_meta['compress'] = "lzw"

            tile_metadata = window_meta
            tile_data = ma.masked_equal(window_data, source_nodata)

        # if tilify, reproject/resample
        if tilify:
            destination_meta = source_meta
            destination_meta['transform'] = destination_affine
            destination_meta['height'] = height
            destination_meta['width'] = width
            destination_meta['compress'] = "lzw"
            try:
                reproject(
                    window_data,
                    destination_data,
                    src_transform=window_affine,
                    src_crs=source_crs,
                    src_nodata=source_nodata,
                    dst_transform=destination_affine,
                    dst_crs=destination_crs,
                    dst_nodata=source_nodata,
                    resampling=RESAMPLING.average)
            except:
                destination_data = None
                destination_meta = None
                raise
            tile_metadata = destination_meta
            tile_data = ma.masked_equal(destination_data, source_nodata)
    except:        
        tile_metadata, tile_data = None, None
        raise

    # return tile metadata and data (numpy)
    return tile_metadata, tile_data


def write_raster_window(output_file,
    tilematrix,
    tileindex,
    metadata,
    bands,
    pixelbuffer=0):

    zoom, row, col = tileindex

    # get write window bounds (i.e. tile bounds plus pixelbuffer) in affine
    out_left, out_bottom, out_right, out_top = tilematrix.tile_bounds(zoom,
        row, col, pixelbuffer)

    out_width = tilematrix.px_per_tile + (pixelbuffer * 2)
    out_height = tilematrix.px_per_tile + (pixelbuffer * 2)
    pixelsize = tilematrix.pixelsize(zoom)
    destination_affine = calculate_default_transform(
        tilematrix.crs,
        tilematrix.crs,
        out_width,
        out_height,
        out_left,
        out_bottom,
        out_right,
        out_top,
        resolution=(pixelsize, pixelsize))[0]

    # convert to pixel coordinates
    input_left = metadata["transform"][2]
    input_top = metadata["transform"][5]
    input_bottom = input_top + (metadata["height"] * metadata["transform"][4])
    input_right = input_left + (metadata["width"] * metadata["transform"][0])
    ul = input_left, input_top
    ur = input_right, input_top
    lr = input_right, input_bottom
    ll = input_left, input_bottom
    px_left = int(round(((out_left - input_left) / pixelsize), 0))
    px_bottom = int(round(((input_top - out_bottom) / pixelsize), 0))
    px_right = int(round(((out_right - input_left) / pixelsize), 0))
    px_top = int(round(((input_top - out_top) / pixelsize), 0))
    window = (px_top, px_bottom), (px_left, px_right)

    # fill with nodata if necessary
    # TODO

    dst_bands = []

    if tilematrix.format.name == "PNG_hillshade":
        zeros = np.zeros(bands[0][px_top:px_bottom, px_left:px_right].shape)
        for band in range(1,4):
            dst_bands.append(zeros)

    for band in bands:
        dst_bands.append(band[px_top:px_bottom, px_left:px_right])

    # write to output file
    dst_metadata = deepcopy(tilematrix.format.profile)
    dst_metadata["crs"] = tilematrix.crs['init']
    dst_metadata["width"] = out_width
    dst_metadata["height"] = out_height
    dst_metadata["transform"] = destination_affine
    dst_metadata["count"] = len(dst_bands)
    dst_metadata["dtype"] = dst_bands[0].dtype.name
    if tilematrix.format.name in ("PNG", "PNG_hillshade"):
        dst_metadata.update(dtype='uint8')
    with rasterio.open(output_file, 'w', **dst_metadata) as dst:
        for band, data in enumerate(dst_bands):
            dst.write_band(
                (band+1),
                data.astype(dst_metadata["dtype"])
            )


def read_vector_window(input_file,
    tilematrix,
    tileindex,
    pixelbuffer=None,
    tilify=True):

    zoom, row, col = tileindex

    assert (isinstance(tilematrix, TileMatrix) or
        isinstance(tilematrix, MetaTileMatrix))

    # read source metadata

    # compute target metadata

    # compute target window

    # open with fiona

    # if tilify, reproject/resample

    # return source metadata and data (shapely)


# auxiliary

def clean_pixel_coordinates(coordinate, maximum):
    # Crops pixel coordinate to 0 or maximum (array.shape) if necessary
    # and returns an offset if necessary.
    offset = None
    if coordinate < 0:
        offset = -coordinate
        coordinate = 0
    if coordinate > maximum:
        offset = coordinate - maximum
        coordinate = maximum
    return coordinate, offset


def raster_bbox(dataset, tilematrix):

    with rasterio.open(dataset) as raster:

        out_left, out_bottom, out_right, out_top = transform_bounds(
            raster.crs, tilematrix.crs, raster.bounds.left,
            raster.bounds.bottom, raster.bounds.right, raster.bounds.top,
            densify_pts=21)

    tl = [out_left, out_top]
    tr = [out_right, out_top]
    br = [out_right, out_bottom]
    bl = [out_left, out_bottom]
    bbox = Polygon([tl, tr, br, bl])

    return bbox