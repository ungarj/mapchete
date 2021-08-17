import fiona
import json
import logging
from multiprocessing import cpu_count
import os
from rasterio.crs import CRS
from shapely.geometry import box, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
from tilematrix import TilePyramid
from typing import Callable, List, Tuple, Union
import warnings

import mapchete
from mapchete.config import _guess_geometry
from mapchete.formats import read_output_metadata
from mapchete.io import fs_from_path, tiles_exist
from mapchete.io.vector import reproject_geometry

logger = logging.getLogger(__name__)


def cp(
    src_tiledir: str,
    dst_tiledir: str,
    zoom: Union[int, List[int]] = None,
    area: Union[BaseGeometry, str, dict] = None,
    area_crs: Union[CRS, str] = None,
    bounds: Tuple[float] = None,
    bounds_crs: Union[CRS, str] = None,
    overwrite: bool = False,
    workers: int = None,
    multi: int = None,
    concurrency: str = None,
    dask_scheduler: str = None,
    dask_client=None,
    src_fs_opts: dict = None,
    dst_fs_opts: dict = None,
    msg_callback: Callable = None,
    as_iterator: bool = False,
) -> mapchete.Job:
    """
    Copy TileDirectory from source to destination.

    Parameters
    ----------
    src_tiledir : str
        Source TileDirectory or mapchete file.
    dst_tiledir : str
        Destination TileDirectory.
    zoom : integer or list of integers
        Single zoom, minimum and maximum zoom or a list of zoom levels.
    area : str, dict, BaseGeometry
        Geometry to override bounds or area provided in process configuration. Can be either a
        WKT string, a GeoJSON mapping, a shapely geometry or a path to a Fiona-readable file.
    area_crs : CRS or str
        CRS of area (default: process CRS).
    bounds : tuple
        Override bounds or area provided in process configuration.
    bounds_crs : CRS or str
        CRS of area (default: process CRS).
    overwrite : bool
        Overwrite existing output.
    workers : int
        Number of threads used to check whether tiles exist.
    concurrency : str
        Concurrency to be used. Could either be "processes", "threads" or "dask".
    dask_scheduler : str
        URL to dask scheduler if required.
    dask_client : dask.distributed.Client
        Reusable Client instance if required. Otherwise a new client will be created.
    src_fs_opts : dict
        Configuration options for source fsspec filesystem.
    dst_fs_opts : dict
        Configuration options for destination fsspec filesystem.
    msg_callback : Callable
        Optional callback function for process messages.
    as_iterator : bool
        Returns as generator but with a __len__() property.

    Returns
    -------
    mapchete.Job instance either with already processed items or a generator with known length.

    Examples
    --------
    >>> cp("foo", "bar", zoom=5)

    This will run the whole copy process.

    >>> for i in cp("foo", "bar", zoom=5, as_iterator=True):
    >>>     print(i)

    This will return a generator where through iteration, tiles are copied.

    >>> list(tqdm.tqdm(cp("foo", "bar", zoom=5, as_iterator=True)))

    Usage within a process bar.
    """

    def _empty_callback(*args):
        pass

    msg_callback = msg_callback or _empty_callback
    if multi is not None:  # pragma: no cover
        warnings.warn("The 'multi' parameter is deprecated and is now named 'workers'")
    workers = workers or multi or cpu_count()
    src_fs_opts = src_fs_opts or {}
    dst_fs_opts = dst_fs_opts or {}
    if zoom is None:  # pragma: no cover
        raise ValueError("zoom level(s) required")

    src_fs = fs_from_path(src_tiledir, **src_fs_opts)
    dst_fs = fs_from_path(dst_tiledir, **dst_fs_opts)

    # open source tile directory
    with mapchete.open(
        src_tiledir,
        zoom=zoom,
        area=area,
        area_crs=area_crs,
        bounds=bounds,
        bounds_crs=bounds_crs,
        fs=src_fs,
        fs_kwargs=src_fs_opts,
    ) as src_mp:
        tp = src_mp.config.output_pyramid

        # copy metadata to destination if necessary
        src_metadata = os.path.join(src_tiledir, "metadata.json")
        dst_metadata = os.path.join(dst_tiledir, "metadata.json")
        if not dst_fs.exists(dst_metadata):
            msg = f"copy {src_metadata} to {dst_metadata}"
            logger.debug(msg)
            msg_callback(msg)
            _copy(src_fs, src_metadata, dst_fs, dst_metadata)

        with mapchete.open(
            dst_tiledir,
            zoom=zoom,
            area=area,
            area_crs=area_crs,
            bounds=bounds,
            bounds_crs=bounds_crs,
            fs=dst_fs,
            fs_kwargs=dst_fs_opts,
        ) as dst_mp:
            return mapchete.Job(
                _copy_tiles,
                fargs=(
                    msg_callback,
                    src_mp,
                    dst_mp,
                    tp,
                    workers,
                    src_fs,
                    dst_fs,
                    overwrite,
                ),
                executor_concurrency=concurrency,
                executor_kwargs=dict(
                    max_workers=workers,
                    dask_scheduler=dask_scheduler,
                    dask_client=dask_client,
                ),
                as_iterator=as_iterator,
                total=src_mp.count_tiles(),
            )


def _copy_tiles(
    msg_callback,
    src_mp,
    dst_mp,
    tp,
    workers,
    src_fs,
    dst_fs,
    overwrite,
    executor=None,
):
    for z in src_mp.config.init_zoom_levels:
        msg_callback(f"copy tiles for zoom {z}...")
        # materialize all tiles
        aoi_geom = src_mp.config.area_at_zoom(z)
        tiles = [
            t
            for t in tp.tiles_from_geom(aoi_geom, z)
            # this is required to omit tiles touching the config area
            if aoi_geom.intersection(t.bbox).area
        ]

        # check which source tiles exist
        logger.debug("looking for existing source tiles...")
        src_tiles_exist = {
            tile: exists
            for tile, exists in tiles_exist(
                config=src_mp.config, output_tiles=tiles, multi=workers
            )
        }

        logger.debug("looking for existing destination tiles...")
        # chech which destination tiles exist
        dst_tiles_exist = {
            tile: exists
            for tile, exists in tiles_exist(
                config=dst_mp.config, output_tiles=tiles, multi=workers
            )
        }

        # copy
        copied = 0
        for future in executor.as_completed(
            _copy_tile,
            tiles,
            fargs=(
                src_mp,
                dst_mp,
                src_tiles_exist,
                dst_tiles_exist,
                src_fs,
                dst_fs,
                overwrite,
            ),
        ):
            c, msg = future.result()
            copied += c
            yield msg

        msg_callback(f"{copied} tiles copied")


def _copy_tile(
    tile, src_mp, dst_mp, src_tiles_exist, dst_tiles_exist, src_fs, dst_fs, overwrite
):
    src_path = src_mp.config.output_reader.get_path(tile)
    # only copy if source tile exists
    if src_tiles_exist[tile]:
        # skip if destination tile exists and overwrite is deactivated
        if dst_tiles_exist[tile] and not overwrite:
            msg = f"{tile}: destination tile exists"
            logger.debug(msg)
            return 0, msg
        # copy from source to target
        else:
            dst_path = dst_mp.config.output_reader.get_path(tile)
            _copy(src_fs, src_path, dst_fs, dst_path)
            msg = f"{tile}: copy {src_path} to {dst_path}"
            logger.debug(msg)
            return 1, msg
    else:
        msg = f"{tile}: source tile ({src_path}) does not exist"
        logger.debug(msg)
        return 0, msg


def _copy(src_fs, src_path, dst_fs, dst_path):
    # create parent directories on local filesystems
    if dst_fs.protocol == "file":
        dst_fs.makedirs(os.path.dirname(dst_path), exist_ok=True)

    # copy either within a filesystem or between filesystems
    if src_fs == dst_fs:
        src_fs.copy(src_path, dst_path)
    else:
        with src_fs.open(src_path, "rb") as src:
            with dst_fs.open(dst_path, "wb") as dst:
                dst.write(src.read())
