"""Wrapper functions around rasterio."""

import os
import rasterio
import numpy as np
import numpy.ma as ma
from rasterio.warp import Resampling, transform_bounds, reproject
from rasterio.windows import from_bounds
from affine import Affine
from tilematrix import clip_geometry_to_srs_bounds

from mapchete import BufferedTile

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


def write_raster_window(
    in_tile=None, out_profile=None, out_tile=None, out_path=None
):
    """
    Write a window from a numpy array to an output file.

    - in_tile: BufferedTile with a data attribute holding NumPy data
    - out_profile: metadata dictionary for rasterio
    - out_tile: provides output boundaries; if None, in_tile is used
    - out_path: output path
    """
    assert isinstance(in_tile, BufferedTile)
    assert isinstance(out_profile, dict)
    if out_tile:
        assert isinstance(out_tile, BufferedTile)
    else:
        out_tile = in_tile
    assert isinstance(out_path, str)
    left, bottom, right, top = out_tile.bounds
    window = from_bounds(
        left, bottom, right, top, in_tile.affine, height=in_tile.height,
        width=in_tile.width)
    minrow = window.row_off
    maxrow = window.row_off + window.num_rows
    mincol = window.col_off
    maxcol = window.col_off + window.num_cols
    window_data = tuple(
        data[minrow:maxrow, mincol:maxcol] for data in in_tile.data)
    # write if there is any band with non-masked data
    if any([band.all() is not ma.masked for band in window_data]):
        with rasterio.open(out_path, 'w', **out_profile) as dst:
            for band, data in enumerate(window_data):
                dst.write(data.astype(out_profile["dtype"]), (band+1))


def create_mosaic(tiles, nodata=0):
    """
    Create a mosaic from tiles.

    - tiles: an iterable containing BufferedTiles
    Returns: (mosaic, affine)
    """
    resolution = None
    dtype = None
    num_bands = 0
    m_left, m_bottom, m_right, m_top = None, None, None, None
    for tile in tiles:
        if isinstance(tile.data, (np.ndarray, ma.MaskedArray)):
            tile.data = (tile.data, )
        num_bands = len(tile.data)
        if resolution is None:
            resolution = tile.pixel_x_size
        if tile.pixel_x_size != resolution:
            raise RuntimeError("tiles must have same resolution")
        if dtype is None:
            dtype = tile.data[0].dtype
        if tile.data[0].dtype != dtype:
            raise RuntimeError("all tiles must have the same dtype")
        left, bottom, right, top = tile.bounds
        m_left = min([left, m_left]) if m_left else left
        m_bottom = min([bottom, m_bottom]) if m_bottom else bottom
        m_right = max([right, m_right]) if m_right else right
        m_top = max([top, m_top]) if m_top else top
    height = int(round((m_top - m_bottom) / resolution))
    width = int(round((m_right - m_left) / resolution))
    mosaic = tuple(
        ma.masked_array(
            data=np.full((height, width), dtype=dtype, fill_value=nodata),
            mask=np.ones((height, width))
        )
        for band in range(num_bands)
    )
    mosaic_affine = Affine.translation(m_left, m_top) * Affine.scale(
        resolution, -resolution)
    for tile in tiles:
        t_left, t_bottom, t_right, t_top = tile.bounds
        window = from_bounds(
            t_left, t_bottom, t_right, t_top, mosaic_affine, height=height,
            width=width)
        minrow = window.row_off
        maxrow = window.row_off + window.num_rows
        mincol = window.col_off
        maxcol = window.col_off + window.num_cols
        for tile_band, mosaic_band in zip(tile.data, mosaic):
            mosaic_band[minrow:maxrow, mincol:maxcol] = tile_band
            mosaic_band.mask[minrow:maxrow, mincol:maxcol] = tile_band.mask
    return (mosaic, mosaic_affine)


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
