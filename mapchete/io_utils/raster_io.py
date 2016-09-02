#!/usr/bin/env python
"""
Raster data read and write functions.
"""

import os
import operator
import rasterio
from rasterio.warp import transform_bounds, reproject
import numpy as np
import numpy.ma as ma
from copy import deepcopy
from affine import Affine
from tilematrix import clip_geometry_to_srs_bounds

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
    Generates numpy arrays (reprojected and resampled to tile properties) from
    input raster. If tile boundaries cross the antimeridian, data on the other
    side of the antimeridian will be read and concatenated to the numpy array
    accordingly.
    input_file: path to a raster file readable by rasterio.
    tile: a Tile object
    pixelbuffer: buffer around tile in pixels.
    indexes: a list of band numbers; None will read all.
    resampling: one of "nearest", "average", "bilinear" or "lanczos"
    """
    try:
        assert os.path.isfile(input_file)
    except AssertionError:
        raise IOError("input file not found %s" % input_file)

    if indexes:
        if isinstance(indexes, list):
            band_indexes = indexes
        else:
            band_indexes = [indexes]
    else:
        with rasterio.open(input_file, "r") as src:
            band_indexes = src.indexes

    # Check if potentially tile boundaries exceed tile matrix boundaries on
    # the antimeridian, the northern or the southern boundary.
    tile_left, tile_bottom, tile_right, tile_top = tile.bounds(pixelbuffer)
    touches_left = tile_left <= tile.tile_pyramid.left
    touches_bottom = tile_bottom <= tile.tile_pyramid.bottom
    touches_right = tile_right >= tile.tile_pyramid.right
    touches_top = tile_top >= tile.tile_pyramid.top
    is_on_edge = touches_left or touches_bottom or touches_right or touches_top
    if pixelbuffer and is_on_edge:
        tile_boxes = clip_geometry_to_srs_bounds(
            tile.bbox(pixelbuffer),
            tile.tile_pyramid,
            multipart=True
            )
        parts_metadata = {}
        parts_metadata.update(
            left=None,
            middle=None,
            right=None,
            none=None
        )
        # Split bounding box into multiple parts & request each numpy array
        # separately.
        for polygon in tile_boxes:
            part_metadata = {}
            # Check on which side the antimeridian is touched by the polygon:
            # "left", "middle", "right"
            # "none" means, the tile touches the edge just on the top and/or
            # bottom boundary
            left, bottom, right, top = polygon.bounds
            touches_right = left == tile.tile_pyramid.left
            touches_left = right == tile.tile_pyramid.right
            touches_both = touches_left and touches_right
            height = int(round((top-bottom)/tile.pixel_y_size))
            width = int(round((right-left)/tile.pixel_x_size))
            affine = Affine.translation(
                left,
                top
                ) * Affine.scale(
                tile.pixel_x_size,
                -tile.pixel_y_size
                )
            part_metadata.update(
                bounds=polygon.bounds,
                shape=(height, width),
                affine=affine
            )
            if touches_both:
                parts_metadata.update(middle=part_metadata)
            elif touches_left:
                parts_metadata.update(left=part_metadata)
            elif touches_right:
                parts_metadata.update(right=part_metadata)
            else:
                parts_metadata.update(none=part_metadata)

        # Finally, stitch numpy arrays together into one.
        for band_idx in band_indexes:
            stitched = ma.concatenate(
                [
                _get_warped_array(
                    input_file=input_file,
                    band_idx=band_idx,
                    dst_bounds=parts_metadata[part]["bounds"],
                    dst_shape=parts_metadata[part]["shape"],
                    dst_affine=parts_metadata[part]["affine"],
                    dst_crs=tile.crs,
                    resampling=resampling
                    )
                for part in ["none", "left", "middle", "right"]
                if parts_metadata[part]
                ],
                axis=1
            )
            assert stitched.shape == tile.shape(pixelbuffer)
            yield stitched

    else:
        # Otherwise, simply read window once.
        for band_idx in band_indexes:
            yield _get_warped_array(
                input_file=input_file,
                band_idx=band_idx,
                dst_bounds=tile.bounds(pixelbuffer),
                dst_shape=tile.shape(pixelbuffer),
                dst_affine=tile.affine(pixelbuffer),
                dst_crs=tile.crs,
                resampling=resampling
                )

def _get_warped_array(
    input_file=None,
    band_idx=None,
    dst_bounds=None,
    dst_shape=None,
    dst_affine=None,
    dst_crs=None,
    resampling="nearest"):
    """
    Extracts a numpy array from a raster file.
    """
    assert isinstance(input_file, str)
    assert isinstance(band_idx, int)
    assert isinstance(dst_bounds, tuple)
    assert isinstance(dst_shape, tuple)
    assert isinstance(dst_affine, Affine)
    assert dst_crs.is_valid

    with rasterio.open(input_file, "r") as src:
        if dst_crs == src.crs:
            src_left, src_bottom, src_right, src_top = dst_bounds
        else:
            # Reproject tile bounds to source file SRS.
            src_left, src_bottom, src_right, src_top = transform_bounds(
                dst_crs,
                src.crs,
                *dst_bounds,
                densify_pts=21
                )
        if float('Inf') in (src_left, src_bottom, src_right, src_top):
            raise RuntimeError(
                "Tile boundaries could not be translated into source SRS."
                )

        # read data window
        window = src.window(
            src_left,
            src_bottom,
            src_right,
            src_top,
            boundless=True
            )
        src_band = src.read(
            band_idx,
            window=window,
            masked=True,
            boundless=True
            )

        # prepare reprojected array
        nodataval = src.nodata
        # Quick fix because None nodata is not allowed.
        if not nodataval:
            nodataval = 0
        dst_band = ma.zeros(
                dst_shape,
                src.dtypes[band_idx-1]
            )
        dst_band[:] = nodataval

        # reproject
        reproject(
            src_band,
            dst_band,
            src_transform=src.window_transform(window),
            src_crs=src.crs,
            src_nodata=nodataval,
            dst_transform=dst_affine,
            dst_crs=dst_crs,
            dst_nodata=nodataval,
            resampling=RESAMPLING_METHODS[resampling]
        )
        return dst_band

def write_raster(
    process,
    metadata,
    bands,
    pixelbuffer=0
    ):
    """
    Function to write arrays to either a NumPy dump or a raster file.
    """

    # NumPy output format requires an array, not a tuple
    if process.output.format == "NumPy":
        # make sure it's an (masked) array
        try:
            assert isinstance(bands, (np.ndarray, ma.MaskedArray))
        except AssertionError:
            raise TypeError(
                "for NumPy output, bands must be provided as arrays"
                )
        # write
        try:
            process.tile.prepare_paths()
            write_numpy(
                process.tile,
                metadata,
                bands,
                pixelbuffer=pixelbuffer
            )
        except:
            raise

    # all other raster outputformats can be fed with a tuple of 2D arrays or a
    # single 2D array
    else:
        # make sure it's a tuple or an array
        if isinstance(bands, (np.ndarray, ma.MaskedArray)):
            bands = (bands, )
        elif isinstance(bands, tuple):
            pass
        else:
            raise TypeError(
                "output bands must be stored in a tuple or a numpy array."
            )
        # make sure bands are 2D arrays
        for band in bands:
            try:
                assert isinstance(band, (np.ndarray, ma.MaskedArray))
            except AssertionError:
                raise TypeError(
                "output bands must be numpy ndarrays or masked arrays, not %s"
                )
            try:
                assert band.ndim == 2
            except AssertionError:
                raise TypeError(
                "output band dimensionality must be 2D"
                )
        # write raster
        try:
            process.tile.prepare_paths()
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

    # spatial clip bands
    clipped_bands = _spatial_clip_bands(bands, tile, pixelbuffer=pixelbuffer)
    for band in clipped_bands:
        assert band.shape == tile.shape(pixelbuffer)

    # adjust band numbers
    dst_bands = _adjust_band_numbers(
        clipped_bands,
        tile.output.format,
        tile.output.bands,
        nodataval=tile.output.nodataval
        )

    dst_metadata = _get_metadata(tile, dst_bands, pixelbuffer=pixelbuffer)

    assert len(dst_bands) == dst_metadata["count"]

    with rasterio.open(output_file, 'w', **dst_metadata) as dst:
        for band, data in enumerate(dst_bands):
            data = np.ma.filled(data, dst_metadata["nodata"])
            dst.write(
                data.astype(dst_metadata["dtype"]),
                (band+1)
            )

def _get_metadata(tile, dst_bands, pixelbuffer=0):
    """
    Returns tile metadata dictionary modified for rasterio write.
    """
    # copy and modify metadata
    dst_width, dst_height = tile.shape(pixelbuffer)
    dst_metadata = deepcopy(tile.output.profile)
    dst_metadata.pop("transform", None)
    dst_metadata.update(
        crs=tile.crs,
        width=dst_width,
        height=dst_height,
        affine=tile.affine(pixelbuffer),
        driver=tile.output.format
    )

    if tile.output.format in ("PNG", "PNG_hillshade"):
        dst_metadata.update(
            dtype='uint8',
            count=len(dst_bands),
            driver="PNG"
        )
    return dst_metadata

def _spatial_clip_bands(bands, tile, pixelbuffer):
    """
    Returns clipped bands so that bands match desired tile output shape.
    """
    # guess target window in source array pixel coordinates
    left, bottom, right, top = tile.bounds(pixelbuffer)
    touches_left = left <= tile.tile_pyramid.left
    touches_bottom = bottom <= tile.tile_pyramid.bottom
    touches_right = right >= tile.tile_pyramid.right
    touches_top = top >= tile.tile_pyramid.top
    is_on_edge = touches_left or touches_bottom or touches_right or touches_top
    if is_on_edge:
        diff_height, diff_width = tuple(
            map(
                operator.sub,
                bands[0].shape,
                tile.shape(pixelbuffer)
                )
            )
        px_left = diff_width/2
        if touches_top:
            px_top = 0
        else:
            px_top = diff_height/2
    else:
        src_pixelbuffer = (bands[0].shape[0] - tile.width) / 2
        px_top, px_left = src_pixelbuffer, src_pixelbuffer
    px_bottom, px_right = tuple(
        map(
            operator.add,
            tile.shape(pixelbuffer),
            (px_top, px_left)
            )
        )
    return [
        band[px_top:px_bottom, px_left:px_right]
        for band in bands
        ]

def _adjust_band_numbers(bands, output_format, bandcount, nodataval=None):
    """
    Adjusts band numbers according to the output format.
    - PNG_hillshade: takes exactly one input band and writes it into an alpha
        band while having otherwise black bands
    - PNG: Asserts, bands values are 8 bit
    """
    dst_bands = ()

    if output_format == "PNG_hillshade":
        try:
            assert len(bands) == 1
        except:
            raise ValueError(
                "only one output band is allowed for PNG_hillshade"
                )
        zeros = np.zeros(bands[0].shape)
        for band in range(1, 4):
            dst_bands += (zeros, )
        dst_bands += (_value_clip_band(bands[0], minval=0, maxval=255), )

    elif output_format == "PNG":
        for band in bands:
            dst_bands += (_value_clip_band(band, minval=0, maxval=255), )
        if nodataval:
            # just add alpha band if there is probably no alpha band yet
            if len(bands) not in [2, 4]:
                nodata_alpha = np.where(
                    bands[0].mask,
                    np.zeros(bands[0].shape),
                    np.full(bands[0].shape, 255)
                    )
                dst_bands += (nodata_alpha, )
            bandcount += 1

        assert len(dst_bands) <= 4

    else:
        dst_bands = bands

    assert len(dst_bands) == bandcount

    return dst_bands


def _value_clip_band(band, minval=0, maxval=255):
    """
    Returns a value clipped version of band.
    """
    return np.clip(band, minval, maxval)
