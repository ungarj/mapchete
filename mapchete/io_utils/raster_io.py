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
from affine import Affine
from shapely.geometry import Polygon, MultiPolygon
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
    touches_left = tile.left-pixelbuffer <= tile.tile_pyramid.left
    touches_bottom = tile.bottom-pixelbuffer <= tile.tile_pyramid.bottom
    touches_right = tile.right+pixelbuffer >= tile.tile_pyramid.right
    touches_top = tile.top+pixelbuffer >= tile.tile_pyramid.top
    is_on_edge = touches_left or touches_bottom or touches_right or touches_top
    if pixelbuffer and is_on_edge:
        tile_boxes = clip_geometry_to_srs_bounds(
            tile.bbox(pixelbuffer),
            tile.tile_pyramid
            )
        if isinstance(tile_boxes, MultiPolygon):
            pass
        elif isinstance(tile_boxes, Polygon):
            tile_boxes = [tile_boxes]
        else:
            raise TypeError("invalid raster window")
        parts_metadata = {}
        parts_metadata.update(
            left=None,
            both=None,
            right=None,
            none=None
        )
        # Prepare top/bottom array extension if necessary.
        touches_top = tile.top+pixelbuffer >= tile.tile_pyramid.top
        touches_bottom = tile.bottom-pixelbuffer <= tile.tile_pyramid.bottom
        if touches_top or touches_bottom:
            with rasterio.open(input_file, "r") as src:
                nodataval = src.nodata
                # Quick fix because None nodata is not allowed.
                if not nodataval:
                    nodataval = 0
                shape = (pixelbuffer, tile.width+2*pixelbuffer)
                nodata_stripe = ma.masked_all(
                    shape,
                    src.dtypes[0]
                    )

        # Split bounding box into multiple parts & request each numpy array
        # separately.
        for polygon in tile_boxes:
            part_metadata = {}
            # Check on which side the antimeridian is touched by the polygon:
            # left, both, right
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
                parts_metadata.update(both=part_metadata)
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
                for part in ["none", "left", "both", "right"]
                if parts_metadata[part]
                ],
                axis=1
            )
            # Extend array to assert it has the expected tile shape.
            #if touches_top:
            #    stitched = ma.concatenate([nodata_stripe, stitched], axis=0)
            #if touches_bottom:
            #    stitched = ma.concatenate([stitched, nodata_stripe], axis=0)
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
    assert isinstance(dst_crs, dict)

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
            raise ValueError(
                "Tile boundaries could not be translated into source file SRS."
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

        # Finally read data.
        src_band = src.read(
            band_idx,
            window=window,
            masked=True,
            boundless=True
            )
        dst_band = ma.zeros(
                dst_shape,
                src.dtypes[band_idx-1]
            )
        dst_band[:] = nodataval
        reproject(
            src_band,
            dst_band,
            src_transform=window_affine,
            src_crs=src.crs,
            src_nodata=nodataval,
            dst_transform=dst_affine,
            dst_crs=dst_crs,
            dst_nodata=nodataval,
            resampling=RESAMPLING_METHODS[resampling]
        )
        return ma.masked_equal(dst_band, nodataval)
        dst_band = ma.masked_equal(dst_band, nodataval)
        dst_band = ma.masked_array(
            dst_band,
            mask=ma.fix_invalid(dst_band, fill_value=0).mask
        )
        dst_band.harden_mask()
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
