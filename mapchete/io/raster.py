"""Wrapper functions around rasterio and useful raster functions."""

import os
import rasterio
import numpy as np
import numpy.ma as ma
from shapely.geometry import box
from rasterio.warp import Resampling, transform_bounds, reproject
from rasterio.windows import from_bounds
from affine import Affine
from tilematrix import clip_geometry_to_srs_bounds

from mapchete.tile import BufferedTile
from mapchete.io.vector import reproject_geometry

RESAMPLING_METHODS = {
    "nearest": Resampling.nearest,
    "bilinear": Resampling.bilinear,
    "cubic": Resampling.cubic,
    "cubic_spline": Resampling.cubic_spline,
    "lanczos": Resampling.lanczos,
    "average": Resampling.average,
    "mode": Resampling.mode
    }


def read_raster_window(input_file, tile, indexes=None, resampling="nearest"):
    """
    Generate NumPy arrays from an input raster.

    NumPy arrays are reprojected and resampled to tile properties from input
    raster. If tile boundaries cross the antimeridian, data on the other side
    of the antimeridian will be read and concatenated to the numpy array
    accordingly.

    Parameters
    ----------
    input_file : string
        path to a raster file readable by rasterio.
    tile : Tile
        a Tile object
    indexes : list or int
        a list of band numbers; None will read all.
    resampling : string
        one of "nearest", "average", "bilinear" or "lanczos"

    Returns
    -------
    raster : MaskedArray
    """
    try:
        assert os.path.isfile(input_file)
    except AssertionError:
        if input_file.split("/")[1] == "vsizip" or (
            input_file.startswith("s3://")
        ):
            pass
        else:
            raise IOError("input file not found %s" % input_file)
    band_indexes = _get_band_indexes(indexes, input_file)
    # Check if potentially tile boundaries exceed tile matrix boundaries on
    # the antimeridian, the northern or the southern boundary.
    if tile.pixelbuffer and _is_on_edge(tile):
        for band_idx in band_indexes:
            yield _get_warped_edge_array(
                tile=tile, input_file=input_file, band_idx=band_idx,
                resampling=resampling)

    # If tile boundaries don't exceed pyramid boundaries, simply read window
    # once.
    else:
        for band_idx in band_indexes:
            yield _get_warped_array(
                input_file=input_file, band_idx=band_idx,
                dst_bounds=tile.bounds, dst_shape=tile.shape,
                dst_affine=tile.affine, dst_crs=tile.crs, resampling=resampling
            )


def _get_warped_edge_array(
    tile=None, input_file=None, band_idx=None, resampling="nearest"
):
    tile_boxes = clip_geometry_to_srs_bounds(
        tile.bbox, tile.tile_pyramid, multipart=True)
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
    return ma.concatenate(
        [
            _get_warped_array(
                input_file=input_file, band_idx=band_idx,
                dst_bounds=parts_metadata[part]["bounds"],
                dst_shape=parts_metadata[part]["shape"],
                dst_affine=parts_metadata[part]["affine"],
                dst_crs=tile.crs, resampling=resampling)
            for part in ["none", "left", "middle", "right"]
            if parts_metadata[part]
        ], axis=1)


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
            # Return empty array if destination bounds don't intersect with
            # file bounds.
            file_bbox = box(*src.bounds)
            tile_bbox = reproject_geometry(
                box(*dst_bounds), src_crs=dst_crs, dst_crs=src.crs)
            if not file_bbox.intersects(tile_bbox):
                return ma.MaskedArray(
                    data=ma.zeros(dst_shape, dtype=src.profile["dtype"]),
                    mask=ma.ones(dst_shape), fill_value=src.nodata)
            # Reproject tile bounds to source file SRS.
            src_left, src_bottom, src_right, src_top = transform_bounds(
                dst_crs, src.crs, *dst_bounds, densify_pts=21)
        if float('Inf') in (src_left, src_bottom, src_right, src_top):
            # Maybe not the best way to deal with it, but if bounding box
            # cannot be translated, it is assumed that data is emtpy
            return ma.MaskedArray(
                data=ma.zeros(dst_shape, dtype=src.profile["dtype"]),
                mask=ma.ones(dst_shape), fill_value=src.nodata)
        # Read data window.
        window = src.window(
            src_left, src_bottom, src_right, src_top, boundless=True)
        src_band = src.read(band_idx, window=window, boundless=True)
        # Quick fix because None nodata is not allowed.
        nodataval = 0 if not src.nodata else src.nodata
        # Prepare reprojected array.
        dst_band = np.empty(dst_shape, src.dtypes[band_idx-1])
        # Run rasterio's reproject().
        reproject(
            src_band, dst_band, src_transform=src.window_transform(window),
            src_crs=src.crs, src_nodata=nodataval, dst_transform=dst_affine,
            dst_crs=dst_crs, dst_nodata=nodataval,
            resampling=RESAMPLING_METHODS[resampling])
        return ma.MaskedArray(dst_band, mask=dst_band == nodataval)


def _is_on_edge(tile):
    """Determine whether tile touches or goes over pyramid edge."""
    tile_left, tile_bottom, tile_right, tile_top = tile.bounds
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


def write_raster_window(
    in_tile=None, out_profile=None, out_tile=None, out_path=None
):
    """
    Write a window from a numpy array to an output file.

    Parameters
    ----------
    in_tile : ``BufferedTile``
        ``BufferedTile`` with a data attribute holding NumPy data
    out_profile : dictionary
        metadata dictionary for rasterio
    out_tile : ``Tile``
        provides output boundaries; if None, in_tile is used
    out_path : string
        output path
    """
    assert isinstance(in_tile, BufferedTile)
    assert isinstance(in_tile.data, ma.MaskedArray)
    assert isinstance(out_profile, dict)
    if out_tile:
        assert isinstance(out_tile, BufferedTile)
    else:
        out_tile = in_tile
    assert isinstance(out_path, str)
    window_data = extract_from_tile(in_tile, out_tile)
    # write if there is any band with non-masked data
    if window_data.all() is not ma.masked:
        with rasterio.open(out_path, 'w', **out_profile) as dst:
            for band, data in enumerate(window_data):
                dst.write(data.astype(out_profile["dtype"]), band+1)


def extract_from_tile(in_tile, out_tile):
    """
    Extract raster data window from BufferedTile.

    Parameters
    ----------
    in_tile : ``BufferedTile``
    out_tile : ``BufferedTile``

    Returns
    -------
    extracted array : array
    """
    assert isinstance(out_tile, BufferedTile)
    assert isinstance(in_tile.data, np.ndarray)
    assert in_tile.data.ndim in [2, 3, 4]
    return extract_from_array(in_tile.data, in_tile.affine, out_tile)


def extract_from_array(in_data, in_affine, out_tile):
    """
    Extract raster data window array.

    Parameters
    ----------
    in_data : array
    in_affine : ``Affine``
    out_tile : ``BufferedTile``

    Returns
    -------
    extracted array : array
    """
    assert isinstance(in_data, np.ndarray)
    left, bottom, right, top = out_tile.bounds
    if in_data.ndim == 2:
        window = from_bounds(
            left, bottom, right, top, in_affine, height=in_data.shape[0],
            width=in_data.shape[1])
    else:
        window = from_bounds(
            left, bottom, right, top, in_affine, height=in_data[0].shape[0],
            width=in_data[0].shape[1])
    minrow = window.row_off
    maxrow = window.row_off + window.num_rows
    mincol = window.col_off
    maxcol = window.col_off + window.num_cols
    if not maxrow > minrow and maxcol > mincol:
        raise ValueError("input and output tile not overlapping")
    if in_data.ndim == 2:
        return in_data[minrow:maxrow, mincol:maxcol]
    else:
        return in_data[..., minrow:maxrow, mincol:maxcol]


def resample_from_array(
    in_data, in_affine, out_tile, resampling="nearest", nodataval=0
):
    """
    Extract and resample from array to target tile.

    Parameters
    ----------
    in_data : array
    in_affine : ``Affine``
    out_tile : ``BufferedTile``
    resampling : string
        one of rasterio's resampling methods (default: nearest)
    nodataval : integer or float
        raster nodata value (default: 0)

    Returns
    -------
    resampled array : array
    """
    if isinstance(in_data, ma.MaskedArray):
        pass
    if isinstance(in_data, np.ndarray):
        in_data = ma.MaskedArray(in_data, mask=in_data == nodataval)
    elif isinstance(in_data, tuple):
        in_data = ma.MaskedArray(
            data=np.stack(in_data),
            mask=np.stack([band.mask for band in in_data]),
            fill_value=nodataval)
    else:
        raise TypeError("wrong input data type: %s" % type(in_data))
    if in_data.ndim == 2:
        in_data = ma.expand_dims(in_data, axis=0)
    elif in_data.ndim == 3:
        pass
    else:
        raise TypeError("input array must have 2 or 3 dimensions")
    if in_data.fill_value != nodataval:
        ma.set_fill_value(in_data, nodataval)
    out_shape = (in_data.shape[0], ) + out_tile.shape
    dst_data = np.empty(out_shape, in_data.dtype)
    in_data = ma.masked_array(
        data=in_data.filled(), mask=in_data.mask, fill_value=nodataval)
    reproject(
        in_data, dst_data, src_transform=in_affine, src_crs=out_tile.crs,
        dst_transform=out_tile.affine, dst_crs=out_tile.crs,
        resampling=RESAMPLING_METHODS[resampling])
    return ma.MaskedArray(dst_data, mask=dst_data == nodataval)


def create_mosaic(tiles, nodata=0):
    """
    Create a mosaic from tiles.

    Parameters
    ----------
    tiles : iterable
        an iterable containing BufferedTiles
    nodata : integer or float
        raster nodata value (default: 0)

    Returns
    -------
    mosaic, affine : tuple
    """
    tiles_list = list(tiles)
    tiles = tiles_list
    if not tiles:
        raise RuntimeError("no tiles provided for mosaic")
    elif len(tiles) == 1:
        return (tiles[0].data, tiles[0].affine)
    resolution = None
    dtype = None
    num_bands = 0
    m_left, m_bottom, m_right, m_top = None, None, None, None
    for tile in tiles:
        if isinstance(tile.data, (np.ndarray, ma.MaskedArray)):
            if tile.data.ndim == 2:
                tile_data = ma.expand_dims(tile.data, axis=0)
            elif tile.data.ndim == 3:
                tile_data = tile.data
            else:
                raise TypeError("tile.data bands must be 2-dimensional")
        elif isinstance(tile.data, tuple):
            tile_data = np.stack(tile.data)
        else:
            raise TypeError("tile.data must be an array or a tuple of arrays")
        if isinstance(tile.data, (np.ndarray)):
            tile.data = ma.masked_where(tile.data == nodata, tile.data)
        num_bands = tile_data.shape[0]
        if resolution is None:
            resolution = tile.pixel_x_size
        if tile.pixel_x_size != resolution:
            raise RuntimeError("tiles must have same resolution")
        if dtype is None:
            dtype = tile_data[0].dtype
        if tile_data[0].dtype != dtype:
            raise RuntimeError("all tiles must have the same dtype")
        left, bottom, right, top = tile.bounds
        m_left = min([left, m_left]) if m_left is not None else left
        m_bottom = min([bottom, m_bottom]) if m_bottom is not None else bottom
        m_right = max([right, m_right]) if m_right is not None else right
        m_top = max([top, m_top]) if m_top is not None else top
    # determine mosaic shape
    height = int(round((m_top - m_bottom) / resolution))
    width = int(round((m_right - m_left) / resolution))
    # initialize empty mosaic
    mosaic = ma.MaskedArray(
            data=np.full(
                (num_bands, height, width), dtype=dtype, fill_value=nodata),
            mask=np.ones((num_bands, height, width)))
    # determine Affine
    mosaic_affine = Affine.translation(m_left, m_top) * Affine.scale(
        resolution, -resolution)
    # fill mosaic array with tile data
    for tile in tiles:
        t_left, t_bottom, t_right, t_top = tile.bounds
        window = from_bounds(
            t_left, t_bottom, t_right, t_top, mosaic_affine, height=height,
            width=width)
        minrow = window.row_off
        maxrow = window.row_off + window.num_rows
        mincol = window.col_off
        maxcol = window.col_off + window.num_cols
        mosaic[:, minrow:maxrow, mincol:maxcol] = tile.data
        mosaic.mask[:, minrow:maxrow, mincol:maxcol] = tile.data.mask
    return (mosaic, mosaic_affine)


def prepare_array(data, masked=True, nodata=0, dtype="int16"):
    """
    Turn input data into a proper array for further usage.

    Outut array is always 3-dimensional with the given data type. If the output
    is masked, the fill_value corresponds to the given nodata value and the
    nodata value will be burned into the data array.

    Parameters
    ----------
    data : array or iterable
        array (masked or normal) or iterable containing arrays
    nodata : integer or float
        nodata value (default: 0) used if input is not a masked array and
        for output array
    masked : bool
        return a NumPy Array or a NumPy MaskedArray (default: True)
    dtype : string
        data type of output array (default: "int16")

    Returns
    -------
    array : array
    """
    # input is iterable
    if isinstance(data, (list, tuple)):
        return _prepare_iterable(data, masked, nodata, dtype)

    # special case if a 2D single band is provided
    elif isinstance(data, np.ndarray) and data.ndim == 2:
        data = ma.expand_dims(data, axis=0)

    # input is a masked array
    if isinstance(data, ma.MaskedArray):
        return _prepare_masked(data, masked, nodata, dtype)

    # input is a NumPy array
    elif isinstance(data, np.ndarray):
        if masked:
            return ma.masked_values(data, nodata).astype(dtype)
        else:
            return data.astype(dtype)
    else:
        raise ValueError(
            "data must be array, masked array or iterable containing arrays.")


def _prepare_iterable(data, masked, nodata, dtype):
    out_data = ()
    out_mask = ()
    for band in data:
        if isinstance(band, ma.MaskedArray):
            try:
                out_data += (band.data, )
                if masked:
                    assert band.shape == band.mask.shape
                    out_mask += (band.mask, )
            except AssertionError:
                out_mask += (
                    np.where(band.data == nodata, True, False), )
        elif isinstance(band, np.ndarray):
            out_data += (band, )
            if masked:
                out_mask += (np.where(band == nodata, True, False), )
        else:
            raise ValueError("input data bands must be NumPy arrays")
    if masked:
        assert len(out_data) == len(out_mask)
        return ma.MaskedArray(
            data=np.stack(out_data).astype(dtype),
            mask=np.stack(out_mask))
    else:
        return np.stack(out_data).astype(dtype)


def _prepare_masked(data, masked, nodata, dtype):
    try:
        assert data.shape == data.mask.shape
        if masked:
            return data.astype(dtype)
        else:
            return ma.filled(data, nodata).astype(dtype)
    except AssertionError:
        if masked:
            return ma.masked_values(data, nodata).astype(dtype)
        else:
            return ma.filled(data, nodata).astype(dtype)
