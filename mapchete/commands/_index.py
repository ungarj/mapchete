import logging
import os
from rasterio.crs import CRS
from shapely.geometry.base import BaseGeometry
import sys
import tqdm
from typing import Callable, List, Tuple, Union

import mapchete
from mapchete.index import zoom_index_gen

logger = logging.getLogger(__name__)


def index(
    tiledir: str,
    idx_out_dir: str = None,
    geojson: bool = False,
    gpkg: bool = False,
    shp: bool = False,
    vrt: bool = False,
    txt: bool = False,
    fieldname: str = "location",
    basepath: str = None,
    for_gdal: bool = False,
    zoom: Union[int, List[int]] = None,
    area: Union[BaseGeometry, str, dict] = None,
    area_crs: Union[CRS, str] = None,
    bounds: Tuple[float] = None,
    bounds_crs: Union[CRS, str] = None,
    point: Tuple[float, float] = None,
    point_crs: Tuple[float, float] = None,
    tile: Tuple[int, int, int] = None,
    fs_opts: dict = None,
    msg_callback: Callable = None,
    as_iterator: bool = False,
    **kwargs,
) -> mapchete.Job:
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
    msg_callback : Callable
        Optional callback function for process messages.
    as_iterator : bool
        Returns as generator but with a __len__() property.

    Returns
    -------
    mapchete.Job instance either with already processed items or a generator with known length.

    Examples
    --------
    >>> index("foo", vrt=True, zoom=5)

    This will run the whole index process.

    >>> for i in index("foo", vrt=True, zoom=5, as_iterator=True):
    >>>     print(i)

    This will return a generator where through iteration, tiles are copied.

    >>> list(tqdm.tqdm(index("foo", vrt=True, zoom=5, as_iterator=True)))

    Usage within a process bar.
    """

    if not any([geojson, gpkg, shp, txt, vrt]):
        raise ValueError(
            """At least one of '--geojson', '--gpkg', '--shp', '--vrt' or '--txt'"""
            """must be provided."""
        )

    def _empty_callback(*args):
        pass

    msg_callback = msg_callback or _empty_callback
    fs_opts = fs_opts or {}

    msg_callback(f"create index(es) for {tiledir}")
    # process single tile
    with mapchete.open(
        tiledir,
        mode="readonly",
        fs_kwargs=fs_opts,
        zoom=tile[0] if tile else zoom,
        point=point,
        point_crs=point_crs,
        bounds=bounds,
        bounds_crs=bounds_crs,
        area=area,
        area_crs=area_crs,
    ) as mp:

        return mapchete.Job(
            zoom_index_gen,
            fkwargs=dict(
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
            as_iterator=as_iterator,
            total=1 if tile else mp.count_tiles(),
        )
