"""Remove tiles from Tile Directory."""

import logging
from typing import List, Optional, Tuple, Union

from rasterio.crs import CRS
from shapely.geometry.base import BaseGeometry

import mapchete
from mapchete.commands.observer import ObserverProtocol, Observers
from mapchete.io import tiles_exist
from mapchete.path import MPath
from mapchete.types import MPathLike, Progress

logger = logging.getLogger(__name__)


def rm(
    tiledir: Optional[MPathLike] = None,
    paths: List[MPath] = None,
    zoom: Union[int, List[int]] = None,
    area: Union[BaseGeometry, str, dict] = None,
    area_crs: Union[CRS, str] = None,
    bounds: Tuple[float] = None,
    bounds_crs: Union[CRS, str] = None,
    workers: Optional[int] = None,
    fs_opts: dict = None,
    observers: Optional[List[ObserverProtocol]] = None,
):
    """
    Remove tiles from TileDirectory.

    Parameters
    ----------
    tiledir : str
        TileDirectory or mapchete file.
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
    fs_opts : dict
        Configuration options for fsspec filesystem.
    """
    all_observers = Observers(observers)

    if tiledir:
        if zoom is None:  # pragma: no cover
            raise ValueError("zoom level(s) required")
        tiledir = MPath.from_inp(tiledir, storage_options=fs_opts)
        paths = existing_paths(
            tiledir=tiledir,
            zoom=zoom,
            area=area,
            area_crs=area_crs,
            bounds=bounds,
            bounds_crs=bounds_crs,
            workers=workers,
        )
        fs = tiledir.fs
    elif isinstance(paths, list):
        fs = MPath.from_inp(paths[0]).fs
    else:  # pragma: no cover
        raise ValueError(
            "either a tile directory or a list of paths has to be provided"
        )

    total = len(paths)
    all_observers.notify(progress=Progress(total=total))
    logger.debug("got %s path(s) on %s", len(paths), fs)

    # s3fs enables multiple paths as input, so let's use this:
    if "s3" in fs.protocol:
        fs.rm(paths)
        for ii, path in enumerate(paths, 1):
            msg = f"deleted {path}"
            logger.debug(msg)
            all_observers.notify(
                progress=Progress(current=ii, total=total), message=msg
            )

    # otherwise, just iterate through the paths
    else:
        for ii, path in enumerate(paths, 1):
            fs.rm(path)
            msg = f"deleted {path}"
            logger.debug(msg)
            all_observers.notify(
                progress=Progress(current=ii, total=total), message=msg
            )

    all_observers.notify(message=f"{len(paths)} tiles deleted")


def existing_paths(
    tiledir: Optional[MPathLike] = None,
    zoom: Union[int, List[int]] = None,
    area: Union[BaseGeometry, str, dict] = None,
    area_crs: Union[CRS, str] = None,
    bounds: Tuple[float] = None,
    bounds_crs: Union[CRS, str] = None,
    workers: Optional[int] = None,
) -> dict:
    with mapchete.open(
        tiledir,
        zoom=zoom,
        area=area,
        area_crs=area_crs,
        bounds=bounds,
        bounds_crs=bounds_crs,
        mode="readonly",
    ) as mp:
        tp = mp.config.output_pyramid
        tiles = {}
        for zoom in mp.config.init_zoom_levels:
            tiles[zoom] = []
            # check which source tiles exist
            logger.debug("looking for existing source tiles in zoom %s...", zoom)
            for tile, exists in tiles_exist(
                config=mp.config,
                output_tiles=[
                    t
                    for t in tp.tiles_from_geom(mp.config.area_at_zoom(zoom), zoom)
                    # this is required to omit tiles touching the config area
                    if mp.config.area_at_zoom(zoom).intersection(t.bbox).area
                ],
                workers=workers,
            ):
                if exists:
                    tiles[zoom].append(tile)

        return [
            mp.config.output_reader.get_path(tile)
            for zoom_tiles in tiles.values()
            for tile in zoom_tiles
        ]
