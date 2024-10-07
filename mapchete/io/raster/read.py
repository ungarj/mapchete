"""Wrapper functions around rasterio and useful raster functions."""

from __future__ import annotations

import logging
import warnings
from contextlib import contextmanager
from typing import Generator, Iterable, List, Optional, Tuple, Union

import numpy as np
import numpy.ma as ma
import rasterio
from affine import Affine
from numpy.typing import DTypeLike
from rasterio.enums import Resampling
from rasterio.errors import RasterioIOError
from rasterio.io import DatasetReader, DatasetWriter, MemoryFile
from rasterio.profiles import Profile
from rasterio.vrt import WarpedVRT
from rasterio.warp import reproject
from retry import retry
from tilematrix import Shape

from mapchete.errors import MapcheteIOError
from mapchete.geometry.clip import clip_geometry_to_pyramid_bounds
from mapchete.grid import Grid
from mapchete.io.raster.array import extract_from_array, prepare_masked_array
from mapchete.io.raster.write import _write_tags
from mapchete.path import MPath
from mapchete.protocols import GridProtocol
from mapchete.settings import IORetrySettings
from mapchete.tile import BufferedTile
from mapchete.timer import Timer
from mapchete.types import MPathLike, NodataVal
from mapchete.validate import validate_write_window_params

logger = logging.getLogger(__name__)


@contextmanager
def rasterio_read(
    path: MPathLike, mode: str = "r", **kwargs
) -> Generator[Union[DatasetReader, DatasetWriter], None, None]:
    """
    Wrapper around rasterio.open but rasterio.Env is set according to path properties.
    """
    path = MPath.from_inp(path)
    with path.rio_env() as env:
        try:
            logger.debug("reading %s with GDAL options %s", str(path), env.options)
            with rasterio.open(path, mode=mode, **kwargs) as src:
                yield src
        except RasterioIOError as rio_exc:
            _extract_filenotfound_exception(rio_exc, path)


def read_raster_window(
    input_files: Union[MPathLike, List[MPathLike]],
    grid: Union[Grid, GridProtocol],
    indexes: Optional[Union[int, List[int]]] = None,
    resampling: Union[Resampling, str] = Resampling.nearest,
    src_nodata: NodataVal = None,
    dst_nodata: NodataVal = None,
    dst_dtype: Optional[DTypeLike] = None,
    gdal_opts: Optional[dict] = None,
    skip_missing_files: bool = False,
) -> ma.MaskedArray:
    """
    Return NumPy arrays from an input raster.

    NumPy arrays are reprojected and resampled to tile properties from input
    raster. If tile boundaries cross the antimeridian, data on the other side
    of the antimeridian will be read and concatenated to the numpy array
    accordingly.
    """
    resampling = (
        resampling if isinstance(resampling, Resampling) else Resampling[resampling]
    )
    input_paths: List[MPath] = [
        MPath.from_inp(input_file)
        for input_file in (
            input_files if isinstance(input_files, list) else [input_files]
        )
    ]
    if len(input_paths) == 0:  # pragma: no cover
        raise ValueError("no input given")

    with input_paths[0].rio_env(gdal_opts) as env:
        logger.debug(
            "reading %s file(s) with GDAL options %s", len(input_paths), env.options
        )
        return _read_raster_window(
            input_paths,
            grid,
            indexes=indexes,
            resampling=resampling,
            src_nodata=src_nodata,
            dst_nodata=dst_nodata,
            dst_dtype=dst_dtype,
            skip_missing_files=skip_missing_files,
        )


def _read_raster_window(
    input_files: List[MPath],
    grid: GridProtocol,
    indexes: Optional[Union[int, List[int]]] = None,
    resampling: Resampling = Resampling.nearest,
    src_nodata: NodataVal = None,
    dst_nodata: NodataVal = None,
    dst_dtype: Optional[DTypeLike] = None,
    skip_missing_files: bool = False,
) -> ma.MaskedArray:
    def _empty_array() -> ma.MaskedArray:
        if indexes is None:  # pragma: no cover
            raise ValueError(
                "output shape cannot be determined because no given input files "
                "exist and no band indexes are given"
            )
        dst_shape = (
            (len(indexes), grid.height, grid.width)
            if isinstance(indexes, list)
            else (grid.height, grid.width)
        )
        return ma.masked_array(
            data=np.full(
                dst_shape,
                src_nodata if dst_nodata is None else dst_nodata,
                dtype=dst_dtype,
            ),
            mask=True,
        )

    if len(input_files) > 1:
        # in case multiple input files are given, merge output into one array
        # using the default rasterio behavior, create a 2D array if only one band
        # is read and a 3D array if multiple bands are read
        dst_array = None
        # read files and add one by one to the output array
        for ff in input_files:
            try:
                f_array = _read_raster_window(
                    [ff],
                    grid=grid,
                    indexes=indexes,
                    resampling=resampling,
                    src_nodata=src_nodata,
                    dst_nodata=dst_nodata,
                )
                if dst_array is None:
                    dst_array = f_array
                else:
                    dst_array[~f_array.mask] = f_array.data[~f_array.mask]
                    dst_array.mask[~f_array.mask] = False
                    logger.debug("added to output array")
            except FileNotFoundError:
                if skip_missing_files:
                    logger.debug("skip missing file %s", ff)
                else:  # pragma: no cover
                    raise
        if dst_array is None:
            dst_array = _empty_array()
        return dst_array
    else:
        input_file = input_files[0]
        try:
            dst_shape = grid.shape
            if not isinstance(indexes, int):
                if indexes is None:
                    dst_shape = (None,) + dst_shape
                elif isinstance(indexes, list):
                    dst_shape = (len(indexes),) + dst_shape
            # Check if potentially tile boundaries exceed tile matrix boundaries on
            # the antimeridian, the northern or the southern boundary.
            if (
                isinstance(grid, BufferedTile)
                and grid.tp.is_global
                and grid.pixelbuffer
                and grid.is_on_edge()
            ):
                return _get_warped_edge_array(
                    tile=grid,
                    input_file=input_file,
                    indexes=indexes,
                    resampling=resampling,
                    src_nodata=src_nodata,
                    dst_nodata=dst_nodata,
                    full_dst_shape=dst_shape,
                )

            # If grid id not a tile or tile boundaries don't exceed pyramid boundaries,
            # simply read window once.
            else:
                return _get_warped_array(
                    input_file=input_file,
                    indexes=indexes,
                    dst_grid=grid,
                    resampling=resampling,
                    src_nodata=src_nodata,
                    dst_nodata=dst_nodata,
                )
        except FileNotFoundError:  # pragma: no cover
            if skip_missing_files:
                logger.debug("skip missing file %s", input_file)
                return _empty_array()
            else:
                raise
        except Exception as exc:  # pragma: no cover
            raise MapcheteIOError(f"failed to read {input_file}") from exc


def _get_warped_edge_array(
    tile: BufferedTile,
    input_file: MPathLike,
    indexes: Optional[Union[int, List[int]]] = None,
    resampling: Resampling = Resampling.nearest,
    src_nodata: NodataVal = None,
    dst_nodata: NodataVal = None,
    full_dst_shape: Optional[Tuple[int, int, int]] = None,
) -> ma.MaskedArray:
    parts_metadata = dict(left=None, middle=None, right=None, none=None)
    # Split bounding box into multiple parts & request each numpy array
    # separately.
    for polygon in clip_geometry_to_pyramid_bounds(tile.bbox, tile.tile_pyramid):
        # Check on which side the antimeridian is touched by the polygon:
        # "left", "middle", "right"
        # "none" means, the tile touches the edge just on the top and/or
        # bottom boundary
        left, bottom, right, top = polygon.bounds
        touches_right = left == tile.tile_pyramid.left
        touches_left = right == tile.tile_pyramid.right
        touches_both = touches_left and touches_right
        height = int(round((top - bottom) / tile.pixel_y_size))
        width = int(round((right - left) / tile.pixel_x_size))
        # if indexes is None:
        #     dst_shape = (None, height, width)
        # elif isinstance(indexes, int):
        #     dst_shape = (height, width)
        # else:
        #     dst_shape = (dst_shape[0], height, width)
        dst_shape = (height, width)
        part_grid = Grid.from_bounds(
            bounds=polygon.bounds, shape=dst_shape, crs=tile.crs
        )
        if touches_both:
            parts_metadata.update(middle=part_grid)
        elif touches_left:
            parts_metadata.update(left=part_grid)
        elif touches_right:
            parts_metadata.update(right=part_grid)
        else:
            parts_metadata.update(none=part_grid)
    # Finally, stitch numpy arrays together into one. Axis -1 is the last axis
    # which in case of rasterio arrays always is the width (West-East).
    return ma.concatenate(
        [
            _get_warped_array(
                input_file=input_file,
                indexes=indexes,
                dst_grid=parts_metadata[part],
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
    input_file: MPathLike,
    dst_grid: GridProtocol,
    indexes: Optional[Union[int, List[int]]] = None,
    resampling: Resampling = Resampling.nearest,
    src_nodata: NodataVal = None,
    dst_nodata: NodataVal = None,
) -> ma.MaskedArray:
    """Extract a numpy array from a raster file."""
    return _rasterio_read(
        input_file=input_file,
        indexes=indexes,
        dst_grid=dst_grid,
        resampling=resampling,
        src_nodata=src_nodata,
        dst_nodata=dst_nodata,
    )


@retry(logger=logger, **dict(IORetrySettings()))
def _rasterio_read(
    input_file: MPathLike,
    dst_grid: GridProtocol,
    indexes: Optional[Union[int, List[int]]] = None,
    resampling: Resampling = Resampling.nearest,
    src_nodata: NodataVal = None,
    dst_nodata: NodataVal = None,
) -> ma.MaskedArray:
    def _read(
        src,
        dst_grid: GridProtocol,
        indexes: Optional[Union[int, List[int]]] = None,
        resampling: Resampling = Resampling.nearest,
        src_nodata: NodataVal = None,
        dst_nodata: NodataVal = None,
    ) -> ma.MaskedArray:
        indexes = indexes or list(src.indexes)
        count = len(indexes) if isinstance(indexes, list) else 1
        dst_array_shape = (count, dst_grid.height, dst_grid.width)
        src_nodata = src.nodata if src_nodata is None else src_nodata
        dst_nodata = src.nodata if dst_nodata is None else dst_nodata
        if src.transform.is_identity and src.gcps:
            # no idea why when reading a source referenced using GCPs requires using reproject()
            # instead of WarpedVRT
            return prepare_masked_array(
                reproject(
                    source=rasterio.band(src, indexes),
                    destination=np.zeros(dst_array_shape, dtype=src.meta.get("dtype")),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    src_nodata=src_nodata,
                    dst_transform=dst_grid.transform,
                    dst_crs=dst_grid.crs,
                    dst_nodata=dst_nodata,
                    resampling=resampling,
                )[0],
                masked=True,
                nodata=dst_nodata,
            )
        else:
            with WarpedVRT(
                src,
                crs=dst_grid.crs,
                src_nodata=src_nodata,
                nodata=dst_nodata,
                width=dst_grid.width,
                height=dst_grid.height,
                transform=dst_grid.transform,
                resampling=resampling,
            ) as vrt:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    return vrt.read(
                        window=vrt.window(*dst_grid.bounds),
                        out_shape=dst_array_shape,
                        indexes=indexes,
                        masked=True,
                    )

    with Timer() as t:
        with rasterio_read(input_file, "r") as src:
            logger.debug("read from %s...", input_file)
            out = _read(
                src,
                dst_grid,
                indexes,
                resampling,
                src_nodata,
                dst_nodata,
            )
    logger.debug("read %s in %s", input_file, t)
    return out


@retry(logger=logger, **dict(IORetrySettings()))
def read_raster_no_crs(
    input_file: MPathLike, indexes: Optional[Union[int, List[int]]] = None, **kwargs
) -> ma.MaskedArray:
    """
    Wrapper function around rasterio.open().read().

    Raises
    ------
    FileNotFoundError if file cannot be found.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            with rasterio_read(input_file, "r") as src:
                return src.read(indexes=indexes, masked=True)
        except FileNotFoundError:
            raise
        except Exception as exc:
            raise MapcheteIOError(exc)


def _extract_filenotfound_exception(rio_exc: Exception, path: MPath):
    """
    Extracts and raises FileNotFoundError from RasterioIOError if applicable.
    """
    filenotfound_msg = (
        f"{str(path)} not found and cannot be opened with rasterio: {str(rio_exc)}"
    )
    # rasterio errors which indicate file does not exist
    for i in (
        "does not exist in the file system",
        "No such file or directory",
        "The specified key does not exist",
    ):
        if i in str(rio_exc):
            raise FileNotFoundError(filenotfound_msg)
    else:
        try:
            # NOTE: this can cause addional S3 requests
            exists = path.exists()
        except Exception:  # pragma: no cover
            # in order not to mask the original rasterio exception, raise it as is
            raise rio_exc
        if exists:
            # raise original rasterio exception
            raise rio_exc
        else:  # pragma: no cover
            # file does not exist
            raise FileNotFoundError(filenotfound_msg)


class RasterWindowMemoryFile:
    """Context manager around rasterio.io.MemoryFile."""

    def __init__(
        self, in_tile=None, in_data=None, out_profile=None, out_tile=None, tags=None
    ):
        """Prepare data & profile."""
        out_tile = out_tile or in_tile
        validate_write_window_params(in_tile, out_tile, in_data, out_profile)
        self.data = extract_from_array(
            array=in_data, in_affine=in_tile.affine, out_tile=out_tile
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


def tiles_to_affine_shape(tiles: Iterable[BufferedTile]) -> Tuple[Affine, Shape]:
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


def memory_file(
    data: np.ndarray, profile: Optional[Union[Profile, dict]] = None
) -> MemoryFile:
    """
    Return a rasterio.io.MemoryFile instance from input.

    Parameters
    ----------
    data : array
        array to be written
    profile : dict
        rasterio profile for MemoryFile
    """
    profile = profile or Profile()
    memfile = MemoryFile()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with memfile.open(
            **dict(profile, width=data.shape[-2], height=data.shape[-1])
        ) as dataset:
            dataset.write(data)
        return memfile
