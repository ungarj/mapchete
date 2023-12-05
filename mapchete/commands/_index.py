"""Create indexes of Tile Directories."""

import logging
from typing import List, Optional, Tuple, Union

from rasterio.crs import CRS
from shapely.geometry.base import BaseGeometry

import mapchete
from mapchete.commands.observer import ObserverProtocol, Observers
from mapchete.config import MapcheteConfig
from mapchete.config.parse import bounds_from_opts, raw_conf, raw_conf_process_pyramid
from mapchete.index import zoom_index_gen
from mapchete.path import MPath
from mapchete.types import MPathLike, Progress

logger = logging.getLogger(__name__)


def index(
    some_input: Union[MPathLike, dict, MapcheteConfig],
    idx_out_dir: Optional[MPathLike] = None,
    geojson: bool = False,
    gpkg: bool = False,
    shp: bool = False,
    vrt: bool = False,
    txt: bool = False,
    fieldname: Optional[str] = "location",
    basepath: Optional[MPathLike] = None,
    for_gdal: bool = False,
    zoom: Optional[Union[int, List[int]]] = None,
    area: Optional[Union[BaseGeometry, str, dict]] = None,
    area_crs: Optional[Union[CRS, str]] = None,
    bounds: Optional[Tuple[float]] = None,
    bounds_crs: Optional[Union[CRS, str]] = None,
    point: Optional[Tuple[float, float]] = None,
    point_crs: Optional[Tuple[float, float]] = None,
    tile: Optional[Tuple[int, int, int]] = None,
    fs_opts: Optional[dict] = None,
    observers: Optional[List[ObserverProtocol]] = None,
    **kwargs,
):
    """
    Create one or more indexes from a TileDirectory.

    Parameters
    ----------
    tiledir : str
        Source TileDirectory or mapchete file.
    idx_out_dir : str
        Alternative output dir for index. Defaults to TileDirectory path.
    geojson : bool
        Activate GeoJSON output.
    gpkg : bool
        Activate GeoPackage output.
    shp : bool
        Activate Shapefile output.
    vrt : bool
        Activate VRT output.
    txt : bool
        Activate TXT output.
    fieldname : str
        Field name which contains paths of tiles (default: "location").
    basepath : str
        Use custom base path for absolute paths instead of output path.
    for_gdal : bool
        Use GDAL compatible remote paths, i.e. add "/vsicurl/" before path.
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
        X and y coordinates of point whose corresponding process tile bounds will be used.
    point_crs : str or CRS
        CRS of point (defaults to process pyramid CRS).
    tile : tuple
        Zoom, row and column of tile to be processed (cannot be used with zoom)
    fs_opts : dict
        Configuration options for fsspec filesystem.
    """

    if not any([geojson, gpkg, shp, txt, vrt]):
        raise ValueError(
            """At least one of '--geojson', '--gpkg', '--shp', '--vrt' or '--txt'"""
            """must be provided."""
        )

    all_observers = Observers(observers)

    all_observers.notify(message=f"create index(es) for {some_input}")

    if tile:
        tile = raw_conf_process_pyramid(raw_conf(some_input)).tile(*tile)
        bounds = tile.bounds
        zoom = tile.zoom
    else:
        try:
            bounds = bounds_from_opts(
                point=point,
                point_crs=point_crs,
                bounds=bounds,
                bounds_crs=bounds_crs,
                raw_conf=raw_conf(some_input),
            )
        except IsADirectoryError:
            pass

    with mapchete.open(
        some_input,
        mode="readonly",
        fs_kwargs=fs_opts,
        zoom=zoom,
        point=point,
        point_crs=point_crs,
        bounds=bounds,
        bounds_crs=bounds_crs,
        area=area,
        area_crs=area_crs,
    ) as mp:
        total = 1 if tile else mp.count_tiles()
        all_observers.notify(progress=Progress(total=total))
        for ii, tile in enumerate(
            zoom_index_gen(
                mp=mp,
                zoom=None if tile else mp.config.init_zoom_levels,
                tile=tile,
                out_dir=idx_out_dir if idx_out_dir else mp.config.output.path,
                geojson=geojson,
                gpkg=gpkg,
                shapefile=shp,
                vrt=vrt,
                txt=txt,
                fieldname=fieldname,
                basepath=basepath,
                for_gdal=for_gdal,
            ),
            1,
        ):
            all_observers.notify(
                progress=Progress(current=ii, total=total), message=f"{tile.id} indexed"
            )
