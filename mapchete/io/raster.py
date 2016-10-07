"""Wrapper functions around rasterio."""

import os
import rasterio
import numpy as np
import numpy.ma as ma
from rasterio.warp import Resampling, transform_bounds, reproject
from affine import Affine
from tilematrix import clip_geometry_to_srs_bounds

RESAMPLING_METHODS = {
    "nearest": Resampling.nearest,
    "bilinear": Resampling.bilinear,
    "cubic": Resampling.cubic,
    "cubic_spline": Resampling.cubic_spline,
    "lanczos": Resampling.lanczos,
    "average": Resampling.average,
    "mode": Resampling.mode
    }


def read_raster_window(
    input_file, tile, indexes=None, pixelbuffer=0, resampling="nearest"
):
    """
    Generate NumPy arrays from an input raster.

    NumPy arrays are reprojected and resampled to tile properties from input
    raster. If tile boundaries cross the antimeridian, data on the other side
    of the antimeridian will be read and concatenated to the numpy array
    accordingly.

    - input_file: path to a raster file readable by rasterio.
    - tile: a Tile object
    - pixelbuffer: buffer around tile in pixels.
    - indexes: a list of band numbers; None will read all.
    - resampling: one of "nearest", "average", "bilinear" or "lanczos"
    """
    try:
        assert os.path.isfile(input_file)
    except AssertionError:
        raise IOError("input file not found %s" % input_file)
    band_indexes = _get_band_indexes(indexes, input_file)
    # Check if potentially tile boundaries exceed tile matrix boundaries on
    # the antimeridian, the northern or the southern boundary.
    if pixelbuffer and _is_on_edge(tile, pixelbuffer):
        tile_boxes = clip_geometry_to_srs_bounds(
            tile.bbox(pixelbuffer), tile.tile_pyramid, multipart=True)
        parts_metadata = dict(left=None, middle=None, right=None, none=None)
        # Split bounding box into multiple parts & request each numpy array
        # separately.
        for polygon in tile_boxes:
            # Check on which side the antimeridian is touched by the polygon:
            # "left", "middle", "right"
            # "none" means, the tile touches the edge just on the top and/or
            # bottom boundary
            part_metadata = {}
            left, bottom, right, top = polygon.bounds
            touches_right = left == tile.tile_pyramid.left
            touches_left = right == tile.tile_pyramid.right
            touches_both = touches_left and touches_right
            height = int(round((top-bottom)/tile.pixel_y_size))
            width = int(round((right-left)/tile.pixel_x_size))
            affine = Affine.translation(
                left, top) * Affine.scale(
                tile.pixel_x_size, -tile.pixel_y_size)
            part_metadata.update(
                bounds=polygon.bounds, shape=(height, width), affine=affine)
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
                        input_file=input_file, band_idx=band_idx,
                        dst_bounds=parts_metadata[part]["bounds"],
                        dst_shape=parts_metadata[part]["shape"],
                        dst_affine=parts_metadata[part]["affine"],
                        dst_crs=tile.crs, resampling=resampling)
                    for part in ["none", "left", "middle", "right"]
                    if parts_metadata[part]
                ],
                axis=1
            )
            assert stitched.shape == tile.shape(pixelbuffer)
            yield stitched

    # If tile boundaries don't exceed pyramid boundaries, simply read window
    # once.
    else:
        for band_idx in band_indexes:
            yield _get_warped_array(
                input_file=input_file, band_idx=band_idx,
                dst_bounds=tile.bounds(pixelbuffer),
                dst_shape=tile.shape(pixelbuffer),
                dst_affine=tile.affine(pixelbuffer),
                dst_crs=tile.crs, resampling=resampling)


def _get_warped_array(
    input_file=None, band_idx=None, dst_bounds=None, dst_shape=None,
    dst_affine=None, dst_crs=None, resampling="nearest"
):
    """Extract a numpy array from a raster file."""
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
                dst_crs, src.crs, *dst_bounds, densify_pts=21)
        if float('Inf') in (src_left, src_bottom, src_right, src_top):
            raise RuntimeError(
                "Tile boundaries could not be translated into source SRS."
                )
        # Read data window.
        window = src.window(
            src_left, src_bottom, src_right, src_top, boundless=True)
        src_band = src.read(
            band_idx, window=window, masked=True, boundless=True)
        # Prepare reprojected array.
        nodataval = src.nodata
        # Quick fix because None nodata is not allowed.
        if not nodataval:
            nodataval = 0
        dst_band = np.zeros(
                dst_shape,
                src.dtypes[band_idx-1]
            )
        dst_band[:] = nodataval
        # Run rasterio's reproject().
        reproject(
            src_band, dst_band, src_transform=src.window_transform(window),
            src_crs=src.crs, src_nodata=nodataval, dst_transform=dst_affine,
            dst_crs=dst_crs, dst_nodata=nodataval,
            resampling=RESAMPLING_METHODS[resampling])
        return ma.MaskedArray(
            dst_band, mask=dst_band == nodataval)


def _is_on_edge(tile, pixelbuffer):
    """Determine whether tile touches or goes over pyramid edge."""
    tile_left, tile_bottom, tile_right, tile_top = tile.bounds(pixelbuffer)
    touches_left = tile_left <= tile.tile_pyramid.left
    touches_bottom = tile_bottom <= tile.tile_pyramid.bottom
    touches_right = tile_right >= tile.tile_pyramid.right
    touches_top = tile_top >= tile.tile_pyramid.top
    return touches_left or touches_bottom or touches_right or touches_top


def _get_band_indexes(indexes, input_file):
    """Return cleaned list of band indexes."""
    if indexes:
        if isinstance(indexes, list):
            return indexes
        else:
            return [indexes]
    else:
        with rasterio.open(input_file, "r") as src:
            return src.indexes
