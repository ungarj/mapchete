"""Wrapper functions around rasterio and useful raster functions."""

import itertools
import rasterio
import logging
import six
import numpy as np
import numpy.ma as ma
from affine import Affine
from collections import namedtuple
from rasterio.enums import Resampling
from rasterio.io import MemoryFile
from rasterio.vrt import WarpedVRT
from rasterio.warp import reproject
from rasterio.windows import from_bounds
from shapely.ops import cascaded_union
from tilematrix import clip_geometry_to_srs_bounds
from types import GeneratorType

from mapchete.tile import BufferedTile
from mapchete.io import path_is_remote


logger = logging.getLogger(__name__)

ReferencedRaster = namedtuple("ReferencedRaster", ("data", "affine"))
GDAL_HTTP_OPTS = dict(
    GDAL_DISABLE_READDIR_ON_OPEN=True,
    GDAL_HTTP_TIMEOUT=30)


def read_raster_window(
    input_file, tile, indexes=None, resampling="nearest", src_nodata=None,
    dst_nodata=None, gdal_opts=None
):
    """
    Return NumPy arrays from an input raster.

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
    src_nodata : int or float, optional
        if not set, the nodata value from the source dataset will be used
    dst_nodata : int or float, optional
        if not set, the nodata value from the source dataset will be used
    gdal_opts : dict
        GDAL options passed on to rasterio.Env()

    Returns
    -------
    raster : MaskedArray
    """
    dst_shape = tile.shape
    user_opts = {} if gdal_opts is None else dict(**gdal_opts)
    if path_is_remote(input_file, s3=True):
        gdal_opts = dict(**GDAL_HTTP_OPTS)
        gdal_opts.update(**user_opts)
    else:
        gdal_opts = user_opts

    if not isinstance(indexes, int):
        if indexes is None:
            dst_shape = (None,) + dst_shape
        elif len(indexes) == 1:
            indexes = indexes[0]
        else:
            dst_shape = (len(indexes),) + dst_shape
    # Check if potentially tile boundaries exceed tile matrix boundaries on
    # the antimeridian, the northern or the southern boundary.
    if tile.pixelbuffer and _is_on_edge(tile):
        return _get_warped_edge_array(
            tile=tile, input_file=input_file, indexes=indexes,
            dst_shape=dst_shape, resampling=resampling, src_nodata=src_nodata,
            dst_nodata=dst_nodata, gdal_opts=gdal_opts
        )

    # If tile boundaries don't exceed pyramid boundaries, simply read window
    # once.
    else:
        return _get_warped_array(
            input_file=input_file, indexes=indexes, dst_bounds=tile.bounds,
            dst_shape=dst_shape, dst_crs=tile.crs, resampling=resampling,
            src_nodata=src_nodata, dst_nodata=dst_nodata, gdal_opts=gdal_opts
        )


def _get_warped_edge_array(
    tile=None, input_file=None, indexes=None, dst_shape=None, resampling=None,
    src_nodata=None, dst_nodata=None, gdal_opts=None
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
        height = int(round((top - bottom) / tile.pixel_y_size))
        width = int(round((right - left) / tile.pixel_x_size))
        if indexes is None:
            dst_shape = (None, height, width)
        elif isinstance(indexes, int):
            dst_shape = (height, width)
        else:
            dst_shape = (dst_shape[0], height, width)
        part_metadata.update(bounds=polygon.bounds, shape=dst_shape)
        if touches_both:
            parts_metadata.update(middle=part_metadata)
        elif touches_left:
            parts_metadata.update(left=part_metadata)
        elif touches_right:
            parts_metadata.update(right=part_metadata)
        else:
            parts_metadata.update(none=part_metadata)
    # Finally, stitch numpy arrays together into one. Axis -1 is the last axis
    # which in case of rasterio arrays always is the width (West-East).
    return ma.concatenate([
        _get_warped_array(
            input_file=input_file, indexes=indexes,
            dst_bounds=parts_metadata[part]["bounds"],
            dst_shape=parts_metadata[part]["shape"],
            dst_crs=tile.crs, resampling=resampling, src_nodata=src_nodata,
            dst_nodata=dst_nodata, gdal_opts=gdal_opts
        )
        for part in ["none", "left", "middle", "right"]
        if parts_metadata[part]
    ], axis=-1)


def _get_warped_array(
    input_file=None, indexes=None, dst_bounds=None, dst_shape=None,
    dst_crs=None, resampling=None, src_nodata=None, dst_nodata=None,
    gdal_opts=None
):
    """Extract a numpy array from a raster file."""
    with rasterio.Env(**gdal_opts):
        with rasterio.open(input_file, "r") as src:
            if indexes is None:
                dst_shape = (len(src.indexes), dst_shape[-2], dst_shape[-1], )
                indexes = list(src.indexes)
            src_nodata = src.nodata if src_nodata is None else src_nodata
            dst_nodata = src.nodata if dst_nodata is None else dst_nodata
            with WarpedVRT(
                src,
                dst_crs=dst_crs,
                src_nodata=src_nodata,
                dst_nodata=dst_nodata,
                dst_width=dst_shape[-2],
                dst_height=dst_shape[-1],
                dst_transform=Affine(
                    (dst_bounds[2] - dst_bounds[0]) / dst_shape[-2],
                    0, dst_bounds[0], 0,
                    (dst_bounds[1] - dst_bounds[3]) / dst_shape[-1],
                    dst_bounds[3]
                ),
                resampling=Resampling[resampling]
            ) as vrt:
                return vrt.read(
                    window=vrt.window(*dst_bounds),
                    out_shape=dst_shape,
                    indexes=indexes,
                    masked=True
                )


def _is_on_edge(tile):
    """Determine whether tile touches or goes over pyramid edge."""
    return any([
        tile.left <= tile.tile_pyramid.left,        # touches_left
        tile.bottom <= tile.tile_pyramid.bottom,    # touches_bottom
        tile.right >= tile.tile_pyramid.right,      # touches_right
        tile.top >= tile.tile_pyramid.top           # touches_top
    ])


class RasterWindowMemoryFile():
    """Context manager around rasterio.io.MemoryFile."""

    def __init__(
        self, in_tile=None, in_data=None, out_profile=None, out_tile=None,
        tags=None
    ):
        """Prepare data & profile."""
        out_tile = in_tile if out_tile is None else out_tile
        _validate_write_window_params(in_tile, out_tile, in_data, out_profile)
        self.data = extract_from_array(
            in_raster=in_data,
            in_affine=in_tile.affine,
            out_tile=out_tile)
        # use transform instead of affine
        if "affine" in out_profile:
            out_profile["transform"] = out_profile.pop("affine")
        self.profile = out_profile
        self.tags = tags

    def __enter__(self):
        """Open MemoryFile, write data and return."""
        self.rio_memfile = MemoryFile()
        with self.rio_memfile.open(**self.profile) as dst:
            for b, d in enumerate(self.data):
                dst.write(d.astype(self.profile["dtype"]), b + 1)
                _write_tags(dst, self.tags)
        return self.rio_memfile

    def __exit__(self, *args):
        """Make sure MemoryFile is closed."""
        self.rio_memfile.close()


def write_raster_window(
    in_tile=None, in_data=None, out_profile=None, out_tile=None, out_path=None,
    tags=None
):
    """
    Write a window from a numpy array to an output file.

    Parameters
    ----------
    in_tile : ``BufferedTile``
        ``BufferedTile`` with a data attribute holding NumPy data
    in_data : array
    out_profile : dictionary
        metadata dictionary for rasterio
    out_tile : ``Tile``
        provides output boundaries; if None, in_tile is used
    out_path : string
        output path to write to; if output path is "memoryfile", a
        rasterio.MemoryFile() is returned
    """
    out_tile = in_tile if out_tile is None else out_tile
    _validate_write_window_params(in_tile, out_tile, in_data, out_profile)
    if not isinstance(out_path, six.string_types):
        raise TypeError("out_path must be a string")
    window_data = extract_from_array(
        in_raster=in_data,
        in_affine=in_tile.affine,
        out_tile=out_tile)
    # use transform instead of affine
    if "affine" in out_profile:
        out_profile["transform"] = out_profile.pop("affine")
    # write if there is any band with non-masked data
    if window_data.all() is not ma.masked:
        if out_path == "memoryfile":
            memfile = MemoryFile()
            with memfile.open(**out_profile) as dst:
                for band, data in enumerate(window_data):
                    dst.write(data.astype(out_profile["dtype"]), band + 1)
                    _write_tags(dst, tags)
            return memfile
        else:
            with rasterio.open(out_path, 'w', **out_profile) as dst:
                for band, data in enumerate(window_data):
                    dst.write(data.astype(out_profile["dtype"]), band + 1)
                    _write_tags(dst, tags)


def _write_tags(dst, tags):
    if tags:
        for k, v in six.iteritems(tags):
            # for band specific tags
            if isinstance(k, int):
                dst.update_tags(k, **v)
            # for filewide tags
            else:
                dst.update_tags(**{k: v})


def _validate_write_window_params(in_tile, out_tile, in_data, out_profile):
    if any([not isinstance(t, BufferedTile) for t in [in_tile, out_tile]]):
        raise TypeError("in_tile and out_tile must be BufferedTile")
    if not isinstance(in_data, ma.MaskedArray):
        raise TypeError("in_data must be ma.MaskedArray")
    if not isinstance(out_profile, dict):
        raise TypeError("out_profile must be a dictionary")


def extract_from_array(in_raster=None, in_affine=None, out_tile=None):
    """
    Extract raster data window array.

    Parameters
    ----------
    in_raster : array or ReferencedRaster
    in_affine : ``Affine`` required if in_raster is an array
    out_tile : ``BufferedTile``

    Returns
    -------
    extracted array : array
    """
    if isinstance(in_raster, ReferencedRaster):
        in_affine = in_raster.affine
        in_raster = in_raster.data

    # get range within array
    minrow, maxrow, mincol, maxcol = _bounds_to_ranges(
        out_tile.bounds, in_affine, in_raster.shape
    )
    # if output window is within input window
    if (
        minrow >= 0 and
        mincol >= 0 and
        maxrow <= in_raster.shape[-2] and
        maxcol <= in_raster.shape[-1]
    ):
        return in_raster[..., minrow:maxrow, mincol:maxcol]
    # raise error if output and input windows do overlap partially
    else:
        raise ValueError(
            "extraction fails if output shape is not within input"
        )


def resample_from_array(
    in_raster=None, in_affine=None, out_tile=None, resampling="nearest",
    nodataval=0
):
    """
    Extract and resample from array to target tile.

    Parameters
    ----------
    in_raster : array
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
    if isinstance(in_raster, ma.MaskedArray):
        pass
    if isinstance(in_raster, np.ndarray):
        in_raster = ma.MaskedArray(in_raster, mask=in_raster == nodataval)
    elif isinstance(in_raster, ReferencedRaster):
        in_affine = in_raster.affine
        in_raster = in_raster.data
    elif isinstance(in_raster, tuple):
        in_raster = ma.MaskedArray(
            data=np.stack(in_raster),
            mask=np.stack([
                band.mask
                if isinstance(band, ma.masked_array)
                else np.where(band == nodataval, True, False)
                for band in in_raster
            ]),
            fill_value=nodataval
        )
    else:
        raise TypeError("wrong input data type: %s" % type(in_raster))
    if in_raster.ndim == 2:
        in_raster = ma.expand_dims(in_raster, axis=0)
    elif in_raster.ndim == 3:
        pass
    else:
        raise TypeError("input array must have 2 or 3 dimensions")
    if in_raster.fill_value != nodataval:
        ma.set_fill_value(in_raster, nodataval)
    out_shape = (in_raster.shape[0], ) + out_tile.shape
    dst_data = np.empty(out_shape, in_raster.dtype)
    in_raster = ma.masked_array(
        data=in_raster.filled(), mask=in_raster.mask, fill_value=nodataval)
    reproject(
        in_raster, dst_data, src_transform=in_affine, src_crs=out_tile.crs,
        dst_transform=out_tile.affine, dst_crs=out_tile.crs,
        resampling=Resampling[resampling])
    return ma.MaskedArray(dst_data, mask=dst_data == nodataval)


def create_mosaic(tiles, nodata=0):
    """
    Create a mosaic from tiles.

    Parameters
    ----------
    tiles : iterable
        an iterable containing tuples of a BufferedTile and an array
    nodata : integer or float
        raster nodata value to initialize the mosaic with (default: 0)

    Returns
    -------
    mosaic : ReferencedRaster
    """
    if isinstance(tiles, GeneratorType):
        tiles = list(tiles)
    elif not isinstance(tiles, list):
        raise TypeError("tiles must be either a list or generator")
    if not all([isinstance(pair, tuple) for pair in tiles]):
        raise TypeError("tiles items must be tuples")
    if not all([
        all([isinstance(tile, BufferedTile), isinstance(data, np.ndarray)])
        for tile, data in tiles
    ]):
        raise TypeError("tuples must be pairs of BufferedTile and array")
    if len(tiles) == 0:
        raise ValueError("tiles list is empty")

    # quick return if there is just one tile
    if len(tiles) == 1:
        tile, data = tiles[0]
        return ReferencedRaster(data=data, affine=tile.affine)

    # assert all tiles have same properties
    pyramid, resolution, dtype = _get_tiles_properties(tiles)
    # just handle antimeridian on global pyramid types
    shift = _shift_required(tiles)
    # determine mosaic shape and reference
    m_left, m_bottom, m_right, m_top = None, None, None, None
    for tile, data in tiles:
        num_bands = data.shape[0] if data.ndim > 2 else 1
        left, bottom, right, top = tile.bounds
        if shift:
            left += pyramid.x_size / 2
            right += pyramid.x_size / 2
            # if tile is now shifted outside pyramid bounds, move within
            if right > pyramid.right:
                right -= pyramid.x_size
                left -= pyramid.x_size
        m_left = min([left, m_left]) if m_left is not None else left
        m_bottom = min([bottom, m_bottom]) if m_bottom is not None else bottom
        m_right = max([right, m_right]) if m_right is not None else right
        m_top = max([top, m_top]) if m_top is not None else top
    height = int(round((m_top - m_bottom) / resolution))
    width = int(round((m_right - m_left) / resolution))
    # initialize empty mosaic
    mosaic = ma.MaskedArray(
        data=np.full(
            (num_bands, height, width), dtype=dtype, fill_value=nodata),
        mask=np.ones((num_bands, height, width))
    )
    # create Affine
    affine = Affine(resolution, 0, m_left, 0, -resolution, m_top)
    # fill mosaic array with tile data
    for tile, data in tiles:
        data = prepare_array(data, nodata=nodata, dtype=dtype)
        t_left, t_bottom, t_right, t_top = tile.bounds
        if shift:
            t_left += pyramid.x_size / 2
            t_right += pyramid.x_size / 2
            # if tile is now shifted outside pyramid bounds, move within
            if t_right > pyramid.right:
                t_right -= pyramid.x_size
                t_left -= pyramid.x_size
        minrow, maxrow, mincol, maxcol = _bounds_to_ranges(
            (t_left, t_bottom, t_right, t_top), affine, (height, width)
        )
        mosaic[:, minrow:maxrow, mincol:maxcol] = data
        mosaic.mask[:, minrow:maxrow, mincol:maxcol] = data.mask
    if shift:
        # shift back output mosaic
        affine = Affine(
            resolution, 0, m_left - pyramid.x_size / 2, 0, -resolution, m_top
        )
    return ReferencedRaster(data=mosaic, affine=affine)


def _bounds_to_ranges(bounds, affine, shape):
    return map(int, itertools.chain(
            *from_bounds(
                *bounds, transform=affine, height=shape[-2], width=shape[-1]
            ).round_lengths().round_offsets().toranges()
        )
    )


def _get_tiles_properties(tiles):
    for tile, data in tiles:
        if tile.zoom != tiles[0][0].zoom:
            raise ValueError("all tiles must be from same zoom level")
        if tile.crs != tiles[0][0].crs:
            raise ValueError("all tiles must have the same CRS")
        if not isinstance(data, (np.ndarray, tuple)):
            raise TypeError("tile data has to be np.ndarray or tuple")
        if data[0].dtype != tiles[0][1][0].dtype:
            raise TypeError("all tile data must have the same dtype")
    return tile.tile_pyramid, tile.pixel_x_size, data[0].dtype


def _shift_required(tiles):
    """Determine if temporary shift is required to deal with antimeridian."""
    if tiles[0][0].tile_pyramid.is_global:
        # check if tiles are all connected to each other
        bbox = cascaded_union([tile.bbox for tile, _ in tiles])
        connected = True if bbox.geom_type != "MultiPolygon" else False
        # if tiles are not connected, shift by half the globe
        return True if not connected else False
    else:
        return False


def memory_file(data=None, profile=None):
    """
    Return a rasterio.io.MemoryFile instance from input.

    Parameters
    ----------
    data : array
        array to be written
    profile : dict
        rasterio profile for MemoryFile
    """
    memfile = MemoryFile()
    profile.update(width=data.shape[-2], height=data.shape[-1])
    with memfile.open(**profile) as dataset:
        dataset.write(data)
    return memfile


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
