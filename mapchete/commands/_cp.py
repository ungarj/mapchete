"""Copy tiles between Tile Directories."""

import logging
from contextlib import AbstractContextManager
from multiprocessing import cpu_count
from typing import List, Optional, Tuple, Union

from rasterio.crs import CRS
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry

import mapchete
from mapchete.commands.observer import ObserverProtocol, Observers
from mapchete.executor import Executor
from mapchete.io import MPath, copy, tiles_exist
from mapchete.io.vector import reproject_geometry
from mapchete.types import Progress

logger = logging.getLogger(__name__)


def cp(
    src_tiledir: Union[str, MPath],
    dst_tiledir: Union[str, MPath],
    zoom: Union[int, List[int]] = None,
    area: Union[BaseGeometry, str, dict] = None,
    area_crs: Union[CRS, str] = None,
    bounds: Tuple[float] = None,
    bounds_crs: Union[CRS, str] = None,
    point: Tuple[float, float] = None,
    point_crs: Tuple[float, float] = None,
    overwrite: bool = False,
    workers: Union[int, None] = None,
    multi: Union[int, None] = None,
    concurrency: Union[str, None] = None,
    dask_scheduler: Union[str, None] = None,
    dask_client=None,
    src_fs_opts: Union[dict, None] = None,
    dst_fs_opts: Union[dict, None] = None,
    executor_getter: AbstractContextManager = Executor,
    observers: Optional[List[ObserverProtocol]] = None,
):
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
    point : iterable
        X and y coordinates of point whose corresponding output tile bounds will be used.
    point_crs : str or CRS
        CRS of point (defaults to process pyramid CRS).
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
    """

    workers = workers or cpu_count()
    src_fs_opts = src_fs_opts or {}
    dst_fs_opts = dst_fs_opts or {}
    if zoom is None:  # pragma: no cover
        raise ValueError("zoom level(s) required")

    src_tiledir = MPath.from_inp(src_tiledir, storage_options=src_fs_opts)
    dst_tiledir = MPath.from_inp(dst_tiledir, storage_options=dst_fs_opts)
    src_fs = src_tiledir.fs
    dst_fs = dst_tiledir.fs
    all_observers = Observers(observers)

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
        mode="readonly",
    ) as src_mp:
        tp = src_mp.config.output_pyramid

        # copy metadata to destination if necessary
        src_metadata = src_tiledir / "metadata.json"
        dst_metadata = dst_tiledir / "metadata.json"
        if not dst_fs.exists(dst_metadata):
            msg = f"copy {src_metadata} to {dst_metadata}"
            logger.debug(msg)
            all_observers.notify(message=msg)
            copy(
                src_metadata,
                dst_metadata,
                src_fs=src_fs,
                dst_fs=dst_fs,
                overwrite=overwrite,
            )

        with mapchete.open(
            dst_tiledir,
            zoom=zoom,
            area=area,
            area_crs=area_crs,
            bounds=bounds,
            bounds_crs=bounds_crs,
            fs=dst_fs,
            fs_kwargs=dst_fs_opts,
            mode="readonly",
        ) as dst_mp:
            with executor_getter(
                concurrency=concurrency,
                max_workers=workers,
                dask_scheduler=dask_scheduler,
                dask_client=dask_client,
            ) as executor:
                for zoom in src_mp.config.init_zoom_levels:
                    all_observers.notify(message=f"copy tiles for zoom {zoom}...")

                    # materialize all tiles
                    if point:
                        point_geom = reproject_geometry(
                            Point(point), src_crs=point_crs or tp.crs, dst_crs=tp.crs
                        )
                        tiles = [tp.tile_from_xy(point_geom.x, point_geom.y, zoom)]
                    else:
                        aoi_geom = src_mp.config.area_at_zoom(zoom)
                        tiles = [
                            t
                            for t in tp.tiles_from_geom(aoi_geom, zoom)
                            # this is required to omit tiles touching the config area
                            if aoi_geom.intersection(t.bbox).area
                        ]

                    all_observers.notify(progress=Progress(current=0, total=len(tiles)))

                    # check which source tiles exist
                    logger.debug("looking for existing source tiles...")
                    src_tiles_exist = dict(
                        tiles_exist(
                            config=src_mp.config, output_tiles=tiles, workers=workers
                        )
                    )

                    # check which destination tiles exist
                    logger.debug("looking for existing destination tiles...")
                    dst_tiles_exist = dict(
                        tiles_exist(
                            config=dst_mp.config, output_tiles=tiles, workers=workers
                        )
                    )

                    # copy
                    total_copied = 0
                    for ii, future in enumerate(
                        executor.as_completed(
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
                        ),
                        1,
                    ):
                        copied, message = future.result()
                        total_copied += copied
                        all_observers.notify(
                            progress=Progress(current=ii, total=len(tiles)),
                            message=message,
                        )

                    all_observers.notify(message=f"{total_copied} tiles copied")


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
        dst_path = dst_mp.config.output_reader.get_path(tile)
        copy(src_path, dst_path, src_fs=src_fs, dst_fs=dst_fs, overwrite=overwrite)
        msg = f"{tile}: copy {src_path} to {dst_path}"
        logger.debug(msg)
        return 1, msg

    msg = f"{tile}: source tile ({src_path}) does not exist"
    logger.debug(msg)
    return 0, msg
