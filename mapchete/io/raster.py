"""Wrapper functions around rasterio and useful raster functions."""

from affine import Affine
from collections import namedtuple
import itertools
import logging
import numpy as np
import numpy.ma as ma
import os
from retry import retry
import rasterio
from rasterio.enums import Resampling
from rasterio.errors import RasterioIOError
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds as affine_from_bounds
from rasterio.vrt import WarpedVRT
from rasterio.warp import reproject
from rasterio.windows import from_bounds
from tempfile import NamedTemporaryFile
from tilematrix import clip_geometry_to_srs_bounds, Shape, Bounds
from types import GeneratorType
import warnings

from mapchete.errors import MapcheteIOError
from mapchete.io import path_is_remote, get_gdal_options, path_exists, fs_from_path
from mapchete.io._misc import MAPCHETE_IO_RETRY_SETTINGS
from mapchete.tile import BufferedTile
from mapchete.validate import validate_write_window_params


logger = logging.getLogger(__name__)

ReferencedRaster = namedtuple("ReferencedRaster", ("data", "affine", "bounds", "crs"))


def read_raster_window(
    input_files,
    tile,
    indexes=None,
    resampling="nearest",
    src_nodata=None,
    dst_nodata=None,
    gdal_opts=None,
):
    """
    Return NumPy arrays from an input raster.

    NumPy arrays are reprojected and resampled to tile properties from input
    raster. If tile boundaries cross the antimeridian, data on the other side
    of the antimeridian will be read and concatenated to the numpy array
    accordingly.

    Parameters
    ----------
    input_files : string or list
        path to a raster file or list of paths to multiple raster files readable by
        rasterio.
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
    try:
        with rasterio.Env(
            **get_gdal_options(
                gdal_opts,
                is_remote=path_is_remote(
                    input_files[0] if isinstance(input_files, list) else input_files,
                    s3=True,
                )
                if isinstance(input_files, str)
                else False,
            )
        ) as env:
            logger.debug("reading %s with GDAL options %s", input_files, env.options)
            return _read_raster_window(
                input_files,
                tile,
                indexes=indexes,
                resampling=resampling,
                src_nodata=src_nodata,
                dst_nodata=dst_nodata,
            )
    except FileNotFoundError:  # pragma: no cover
        raise
    except Exception as e:  # pragma: no cover
        raise MapcheteIOError(e)


def _read_raster_window(
    input_files,
    tile,
    indexes=None,
    resampling="nearest",
    src_nodata=None,
    dst_nodata=None,
):
    if isinstance(input_files, list):
        # in case multiple input files are given, merge output into one array
        # read first file
        dst_array = _read_raster_window(
            input_files[0],
            tile=tile,
            indexes=indexes,
            resampling=resampling,
            src_nodata=src_nodata,
            dst_nodata=dst_nodata,
        )
        # read subsequent files and merge
        for f in input_files[1:]:
            f_array = _read_raster_window(
                f,
                tile=tile,
                indexes=indexes,
                resampling=resampling,
                src_nodata=src_nodata,
                dst_nodata=dst_nodata,
            )
            dst_array = ma.MaskedArray(
                data=np.where(f_array.mask, dst_array, f_array).astype(
                    dst_array.dtype, copy=False
                ),
                mask=np.where(f_array.mask, dst_array.mask, f_array.mask).astype(
                    bool, copy=False
                ),
            )
        return dst_array
    else:
        input_file = input_files
        dst_shape = tile.shape

        if not isinstance(indexes, int):
            if indexes is None:
                dst_shape = (None,) + dst_shape
            elif len(indexes) == 1:
                indexes = indexes[0]
            else:
                dst_shape = (len(indexes),) + dst_shape
        # Check if potentially tile boundaries exceed tile matrix boundaries on
        # the antimeridian, the northern or the southern boundary.
        if tile.tp.is_global and tile.pixelbuffer and tile.is_on_edge():
            return _get_warped_edge_array(
                tile=tile,
                input_file=input_file,
                indexes=indexes,
                dst_shape=dst_shape,
                resampling=resampling,
                src_nodata=src_nodata,
                dst_nodata=dst_nodata,
            )

        # If tile boundaries don't exceed pyramid boundaries, simply read window
        # once.
        else:
            return _get_warped_array(
                input_file=input_file,
                indexes=indexes,
                dst_bounds=tile.bounds,
                dst_shape=dst_shape,
                dst_crs=tile.crs,
                resampling=resampling,
                src_nodata=src_nodata,
                dst_nodata=dst_nodata,
            )


def _get_warped_edge_array(
    tile=None,
    input_file=None,
    indexes=None,
    dst_shape=None,
    resampling=None,
    src_nodata=None,
    dst_nodata=None,
):
    tile_boxes = clip_geometry_to_srs_bounds(
        tile.bbox, tile.tile_pyramid, multipart=True
    )
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
    return ma.concatenate(
        [
            _get_warped_array(
                input_file=input_file,
                indexes=indexes,
                dst_bounds=parts_metadata[part]["bounds"],
                dst_shape=parts_metadata[part]["shape"],
                dst_crs=tile.crs,
                resampling=resampling,
                src_nodata=src_nodata,
                dst_nodata=dst_nodata,
            )
            for part in ["none", "left", "middle", "right"]
            if parts_metadata[part]
        ],
        axis=-1,
    )


def _get_warped_array(
    input_file=None,
    indexes=None,
    dst_bounds=None,
    dst_shape=None,
    dst_crs=None,
    resampling=None,
    src_nodata=None,
    dst_nodata=None,
):
    """Extract a numpy array from a raster file."""
    try:
        return _rasterio_read(
            input_file=input_file,
            indexes=indexes,
            dst_bounds=dst_bounds,
            dst_shape=dst_shape,
            dst_crs=dst_crs,
            resampling=resampling,
            src_nodata=src_nodata,
            dst_nodata=dst_nodata,
        )
    except Exception as e:
        logger.exception("error while reading file %s: %s", input_file, e)
        raise


@retry(logger=logger, exceptions=RasterioIOError, **MAPCHETE_IO_RETRY_SETTINGS)
def _rasterio_read(
    input_file=None,
    indexes=None,
    dst_bounds=None,
    dst_shape=None,
    dst_crs=None,
    resampling=None,
    src_nodata=None,
    dst_nodata=None,
):
    def _read(
        src, indexes, dst_bounds, dst_shape, dst_crs, resampling, src_nodata, dst_nodata
    ):
        height, width = dst_shape[-2:]
        if indexes is None:
            dst_shape = (len(src.indexes), height, width)
            indexes = list(src.indexes)
        src_nodata = src.nodata if src_nodata is None else src_nodata
        dst_nodata = src.nodata if dst_nodata is None else dst_nodata
        dst_left, dst_bottom, dst_right, dst_top = dst_bounds
        with WarpedVRT(
            src,
            crs=dst_crs,
            src_nodata=src_nodata,
            nodata=dst_nodata,
            width=width,
            height=height,
            transform=affine_from_bounds(
                dst_left, dst_bottom, dst_right, dst_top, width, height
            ),
            resampling=Resampling[resampling],
        ) as vrt:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return vrt.read(
                    window=vrt.window(*dst_bounds),
                    out_shape=dst_shape,
                    indexes=indexes,
                    masked=True,
                )

    if isinstance(input_file, str):
        logger.debug("got file path %s", input_file)
        try:
            with rasterio.open(input_file, "r") as src:
                return _read(
                    src,
                    indexes,
                    dst_bounds,
                    dst_shape,
                    dst_crs,
                    resampling,
                    src_nodata,
                    dst_nodata,
                )
        except RasterioIOError as e:
            try:
                if path_exists(input_file):
                    raise e
            except Exception:
                raise e
            raise FileNotFoundError("%s not found" % input_file)
    else:  # pragma: no cover
        logger.debug("assuming file object %s", input_file)
        warnings.warn(
            "passing on a rasterio dataset object is not recommended, see "
            "https://github.com/mapbox/rasterio/issues/1309"
        )
        return _read(
            input_file,
            indexes,
            dst_bounds,
            dst_shape,
            dst_crs,
            resampling,
            src_nodata,
            dst_nodata,
        )


@retry(logger=logger, exceptions=RasterioIOError, **MAPCHETE_IO_RETRY_SETTINGS)
def read_raster_no_crs(input_file, indexes=None, gdal_opts=None):
    """
    Wrapper function around rasterio.open().read().

    Parameters
    ----------
    input_file : str
        Path to file
    indexes : int or list
        Band index or list of band indexes to be read.
    gdal_opts : dict
        GDAL options passed on to rasterio.Env()

    Returns
    -------
    MaskedArray

    Raises
    ------
    FileNotFoundError if file cannot be found.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            with rasterio.Env(
                **get_gdal_options(
                    gdal_opts,
                    is_remote=path_is_remote(input_file, s3=True),
                    allowed_remote_extensions=os.path.splitext(input_file)[1],
                ),
            ) as env:
                logger.debug("reading %s with GDAL options %s", input_file, env.options)
                with rasterio.open(input_file, "r") as src:
                    return src.read(indexes=indexes, masked=True)
        except RasterioIOError as e:
            try:
                if path_exists(input_file):
                    raise MapcheteIOError(e)
            except Exception:
                raise MapcheteIOError(e)
            raise FileNotFoundError("%s not found" % input_file)


class RasterWindowMemoryFile:
    """Context manager around rasterio.io.MemoryFile."""

    def __init__(
        self, in_tile=None, in_data=None, out_profile=None, out_tile=None, tags=None
    ):
        """Prepare data & profile."""
        out_tile = out_tile or in_tile
        validate_write_window_params(in_tile, out_tile, in_data, out_profile)
        self.data = extract_from_array(
            in_raster=in_data, in_affine=in_tile.affine, out_tile=out_tile
        )
        # use transform instead of affine
        if "affine" in out_profile:
            out_profile["transform"] = out_profile.pop("affine")
        self.profile = out_profile
        self.tags = tags

    def __enter__(self):
        """Open MemoryFile, write data and return."""
        self.rio_memfile = MemoryFile()
        with self.rio_memfile.open(**self.profile) as dst:
            dst.write(self.data.astype(self.profile["dtype"], copy=False))
            _write_tags(dst, self.tags)
        return self.rio_memfile

    def __exit__(self, *args):
        """Make sure MemoryFile is closed."""
        self.rio_memfile.close()


def write_raster_window(
    in_tile=None,
    in_data=None,
    out_profile=None,
    out_tile=None,
    out_path=None,
    tags=None,
    fs=None,
    **kwargs,
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
        output path to write to
    tags : optional tags to be added to GeoTIFF file
    """
    if not isinstance(out_path, str):
        raise TypeError("out_path must be a string")
    logger.debug("write %s", out_path)
    if out_path == "memoryfile":
        raise DeprecationWarning(
            "Writing to memoryfile with write_raster_window() is deprecated. "
            "Please use RasterWindowMemoryFile."
        )
    out_tile = out_tile or in_tile
    validate_write_window_params(in_tile, out_tile, in_data, out_profile)

    # extract data
    window_data = (
        extract_from_array(
            in_raster=in_data, in_affine=in_tile.affine, out_tile=out_tile
        )
        if in_tile != out_tile
        else in_data
    )

    # use transform instead of affine
    if "affine" in out_profile:
        out_profile["transform"] = out_profile.pop("affine")

    # write if there is any band with non-masked data
    if window_data.all() is not ma.masked:

        try:
            with rasterio_write(out_path, "w", fs=fs, **out_profile) as dst:
                logger.debug((out_tile.id, "write tile", out_path))
                dst.write(window_data.astype(out_profile["dtype"], copy=False))
                _write_tags(dst, tags)
        except Exception as e:
            logger.exception("error while writing file %s: %s", out_path, e)
            raise
    else:
        logger.debug((out_tile.id, "array window empty", out_path))


def _write_tags(dst, tags):
    if tags:
        for k, v in tags.items():
            # for band specific tags
            if isinstance(k, int):
                dst.update_tags(k, **v)
            # for filewide tags
            else:
                dst.update_tags(**{k: v})


def rasterio_write(path, mode=None, fs=None, in_memory=True, *args, **kwargs):
    """
    Wrap rasterio.open() but handle bucket upload if path is remote.

    Parameters
    ----------
    path : str
        Path to write to.
    mode : str
        One of the rasterio.open() modes.
    fs : fsspec.FileSystem
        Target filesystem.
    in_memory : bool
        On remote output store an in-memory file instead of writing to a tempfile.
    args : list
        Arguments to be passed on to rasterio.open()
    kwargs : dict
        Keyword arguments to be passed on to rasterio.open()

    Returns
    -------
    RasterioRemoteWriter if target is remote, otherwise return rasterio.open().
    """
    if path.startswith("s3://"):
        return RasterioRemoteWriter(path, fs=fs, in_memory=in_memory, *args, **kwargs)
    else:
        return rasterio.open(path, mode=mode, *args, **kwargs)


class RasterioRemoteWriter:
    def __init__(self, path, *args, fs=None, in_memory=True, **kwargs):
        logger.debug("open RasterioRemoteWriter for path %s", path)
        self.path = path
        self.fs = fs or fs_from_path(path)
        self.in_memory = in_memory
        if self.in_memory:
            self._dst = MemoryFile()
        else:
            self._dst = NamedTemporaryFile(suffix=".tif")
        self._open_args = args
        self._open_kwargs = kwargs
        self._sink = None

    def __enter__(self):
        if self.in_memory:
            self._sink = self._dst.open(*self._open_args, **self._open_kwargs)
        else:
            self._sink = rasterio.open(
                self._dst.name, "w+", *self._open_args, **self._open_kwargs
            )
        return self._sink

    def __exit__(self, *args):
        try:
            self._sink.close()
            if self.in_memory:
                logger.debug("write rasterio MemoryFile to %s", self.path)
                with self.fs.open(self.path, "wb") as dst:
                    dst.write(self._dst.getbuffer())
            else:
                self.fs.put_file(self._dst.name, self.path)
        finally:
            if self.in_memory:
                logger.debug("close rasterio MemoryFile")
            else:
                logger.debug("close and remove tempfile")
            self._dst.close()


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
    if isinstance(in_raster, ReferencedRaster):  # pragma: no cover
        in_affine, in_raster = in_raster.affine, in_raster.data

    # get range within array
    minrow, maxrow, mincol, maxcol = bounds_to_ranges(
        out_bounds=out_tile.bounds, in_affine=in_affine, in_shape=in_raster.shape
    )
    # if output window is within input window
    if (
        minrow >= 0
        and mincol >= 0
        and maxrow <= in_raster.shape[-2]
        and maxcol <= in_raster.shape[-1]
    ):
        return in_raster[..., minrow:maxrow, mincol:maxcol]
    # raise error if output is not fully within input
    else:
        raise ValueError("extraction fails if output shape is not within input")


def resample_from_array(
    in_raster=None,
    in_affine=None,
    out_tile=None,
    in_crs=None,
    resampling="nearest",
    nodataval=None,
    nodata=0,
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
    nodata : integer or float
        raster nodata value (default: 0)

    Returns
    -------
    resampled array : array
    """
    if nodataval is not None:  # pragma: no cover
        warnings.warn("'nodataval' is deprecated, please use 'nodata'")
        nodata = nodata or nodataval
    # TODO rename function
    if isinstance(in_raster, ma.MaskedArray):
        pass
    elif isinstance(in_raster, np.ndarray):
        in_raster = ma.MaskedArray(in_raster, mask=in_raster == nodata)
    elif isinstance(in_raster, ReferencedRaster):
        in_affine = in_raster.affine
        in_crs = in_raster.crs
        in_raster = in_raster.data
    elif isinstance(in_raster, tuple):
        in_raster = ma.MaskedArray(
            data=np.stack(in_raster),
            mask=np.stack(
                [
                    band.mask
                    if isinstance(band, ma.masked_array)
                    else np.where(band == nodata, True, False)
                    for band in in_raster
                ]
            ),
            fill_value=nodata,
        )
    else:
        raise TypeError("wrong input data type: %s" % type(in_raster))
    if in_raster.ndim == 2:
        in_raster = ma.expand_dims(in_raster, axis=0)
    elif in_raster.ndim == 3:
        pass
    else:
        raise TypeError("input array must have 2 or 3 dimensions")
    if in_raster.fill_value != nodata:
        ma.set_fill_value(in_raster, nodata)
    dst_data = np.empty((in_raster.shape[0],) + out_tile.shape, in_raster.dtype)
    reproject(
        in_raster.filled(),
        dst_data,
        src_transform=in_affine,
        src_crs=in_crs or out_tile.crs,
        src_nodata=nodata,
        dst_transform=out_tile.affine,
        dst_crs=out_tile.crs,
        dst_nodata=nodata,
        resampling=Resampling[resampling],
    )
    return ma.MaskedArray(dst_data, mask=dst_data == nodata, fill_value=nodata)


def create_mosaic(tiles, nodata=0):
    """
    Create a mosaic from tiles.

    Tiles must be connected (also possible over Antimeridian), otherwise strange things
    can happen!

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
    if not all(
        [
            all([isinstance(tile, BufferedTile), isinstance(data, np.ndarray)])
            for tile, data in tiles
        ]
    ):
        raise TypeError("tuples must be pairs of BufferedTile and array")
    if len(tiles) == 0:
        raise ValueError("tiles list is empty")

    logger.debug("create mosaic from %s tile(s)", len(tiles))
    # quick return if there is just one tile
    if len(tiles) == 1:
        tile, data = tiles[0]
        return ReferencedRaster(
            data=data, affine=tile.affine, bounds=tile.bounds, crs=tile.crs
        )

    # assert all tiles have same properties
    pyramid, resolution, dtype = _get_tiles_properties(tiles)
    # just handle antimeridian on global pyramid types
    shift = _shift_required(tiles)
    logger.debug("shift: %s" % shift)
    # determine mosaic shape and reference
    m_left, m_bottom, m_right, m_top = None, None, None, None
    for tile, data in tiles:
        num_bands = data.shape[0] if data.ndim > 2 else 1
        left, bottom, right, top = tile.bounds
        if shift:
            # shift by half of the grid width
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
        data=np.full((num_bands, height, width), dtype=dtype, fill_value=nodata),
        mask=np.ones((num_bands, height, width)),
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
        minrow, maxrow, mincol, maxcol = bounds_to_ranges(
            out_bounds=(t_left, t_bottom, t_right, t_top),
            in_affine=affine,
            in_shape=(height, width),
        )
        existing_data = mosaic[:, minrow:maxrow, mincol:maxcol]
        existing_mask = mosaic.mask[:, minrow:maxrow, mincol:maxcol]
        mosaic[:, minrow:maxrow, mincol:maxcol] = np.where(
            data.mask, existing_data, data
        )
        mosaic.mask[:, minrow:maxrow, mincol:maxcol] = np.where(
            data.mask, existing_mask, data.mask
        )

    if shift:
        # shift back output mosaic
        m_left -= pyramid.x_size / 2
        m_right -= pyramid.x_size / 2

    # if mosaic crosses Antimeridian, make sure the mosaic output bounds are based on the
    # hemisphere of the Antimeridian with the larger mosaic intersection
    if m_left < pyramid.left or m_right > pyramid.right:
        # mosaic crosses Antimeridian
        logger.debug("mosaic crosses Antimeridian")
        left_distance = abs(pyramid.left - m_left)
        right_distance = abs(pyramid.left - m_right)
        # per default, the mosaic is placed on the right side of the Antimeridian, so we
        # only need to move the bounds in case the larger part of the mosaic is on the
        # left side
        if left_distance > right_distance:
            m_left += pyramid.x_size
            m_right += pyramid.x_size
    logger.debug(Bounds(m_left, m_bottom, m_right, m_top))
    return ReferencedRaster(
        data=mosaic,
        affine=Affine(resolution, 0, m_left, 0, -resolution, m_top),
        bounds=Bounds(m_left, m_bottom, m_right, m_top),
        crs=tile.crs,
    )


def bounds_to_ranges(out_bounds=None, in_affine=None, in_shape=None):
    """
    Return bounds range values from geolocated input.

    Parameters
    ----------
    out_bounds : tuple
        left, bottom, right, top
    in_affine : Affine
        input geolocation
    in_shape : tuple
        input shape

    Returns
    -------
    minrow, maxrow, mincol, maxcol
    """
    return itertools.chain(
        *from_bounds(
            *out_bounds, transform=in_affine, height=in_shape[-2], width=in_shape[-1]
        )
        .round_lengths(pixel_precision=0)
        .round_offsets(pixel_precision=0)
        .toranges()
    )


def tiles_to_affine_shape(tiles):
    """
    Return Affine and shape of combined tiles.

    Parameters
    ----------
    tiles : iterable
        an iterable containing BufferedTiles

    Returns
    -------
    Affine, Shape
    """
    if not tiles:  # pragma: no cover
        raise TypeError("no tiles provided")
    pixel_size = tiles[0].pixel_x_size
    left, bottom, right, top = (
        min([t.left for t in tiles]),
        min([t.bottom for t in tiles]),
        max([t.right for t in tiles]),
        max([t.top for t in tiles]),
    )
    return (
        Affine(pixel_size, 0, left, 0, -pixel_size, top),
        Shape(
            width=int(round((right - left) / pixel_size, 0)),
            height=int(round((top - bottom) / pixel_size, 0)),
        ),
    )


def _get_tiles_properties(tiles):
    for tile, data in tiles:
        if tile.zoom != tiles[0][0].zoom:
            raise ValueError("all tiles must be from same zoom level")
        if tile.crs != tiles[0][0].crs:
            raise ValueError("all tiles must have the same CRS")
        if isinstance(data, np.ndarray):
            if data[0].dtype != tiles[0][1][0].dtype:
                raise TypeError("all tile data must have the same dtype")
    return tile.tile_pyramid, tile.pixel_x_size, data[0].dtype


def _shift_required(tiles):
    """Determine if distance over antimeridian is shorter than normal distance."""
    if tiles[0][0].tile_pyramid.is_global:
        # get set of tile columns
        tile_cols = sorted(list(set([t[0].col for t in tiles])))
        # if tile columns are an unbroken sequence, tiles are connected and are not
        # passing the Antimeridian
        if tile_cols == list(range(min(tile_cols), max(tile_cols) + 1)):
            return False
        else:
            # look at column gaps and try to determine the smallest distance
            def gen_groups(items):
                """Group tile columns by sequence."""
                j = items[0]
                group = [j]
                for i in items[1:]:
                    # item is next in expected sequence
                    if i == j + 1:
                        group.append(i)
                    # gap occured, so yield existing group and create new one
                    else:
                        yield group
                        group = [i]
                    j = i
                yield group

            groups = list(gen_groups(tile_cols))
            # in case there is only one group, don't shift
            if len(groups) == 1:  # pragma: no cover
                return False
            # distance between first column of first group and last column of last group
            normal_distance = groups[-1][-1] - groups[0][0]
            # distance between last column of first group and last column of first group
            # but crossing the antimeridian
            antimeridian_distance = (
                groups[0][-1] + tiles[0][0].tile_pyramid.matrix_width(tiles[0][0].zoom)
            ) - groups[-1][0]
            # return whether distance over antimeridian is shorter
            return antimeridian_distance < normal_distance
    else:  # pragma: no cover
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
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with memfile.open(
            **dict(profile, width=data.shape[-2], height=data.shape[-1])
        ) as dataset:
            dataset.write(data)
        return memfile


def prepare_array(data, masked=True, nodata=0, dtype="int16"):
    """
    Turn input data into a proper array for further usage.

    Output array is always 3-dimensional with the given data type. If the output
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
            return ma.masked_values(data.astype(dtype, copy=False), nodata, copy=False)
        else:
            return data.astype(dtype, copy=False)
    else:
        raise ValueError(
            "Data must be array, masked array or iterable containing arrays. "
            "Current data: %s (%s)" % (data, type(data))
        )


def _prepare_iterable(data, masked, nodata, dtype):
    out_data = ()
    out_mask = ()
    for band in data:
        if isinstance(band, ma.MaskedArray):
            out_data += (band.data,)
            if masked:
                if band.shape == band.mask.shape:
                    out_mask += (band.mask,)
                else:
                    out_mask += (np.where(band.data == nodata, True, False),)
        elif isinstance(band, np.ndarray):
            out_data += (band,)
            if masked:
                out_mask += (np.where(band == nodata, True, False),)
        else:
            raise ValueError("input data bands must be NumPy arrays")
    if masked:
        return ma.MaskedArray(
            data=np.stack(out_data).astype(dtype, copy=False), mask=np.stack(out_mask)
        )
    else:
        return np.stack(out_data).astype(dtype, copy=False)


def _prepare_masked(data, masked, nodata, dtype):
    if data.shape == data.mask.shape:
        if masked:
            return ma.masked_values(data.astype(dtype, copy=False), nodata, copy=False)
        else:
            return ma.filled(data.astype(dtype, copy=False), nodata)
    else:
        if masked:
            return ma.masked_values(data.astype(dtype, copy=False), nodata, copy=False)
        else:
            return ma.filled(data.astype(dtype, copy=False), nodata)
