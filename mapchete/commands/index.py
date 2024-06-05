"""Create indexes of Tile Directories."""

import logging
from typing import List, Optional, Tuple, Union

from rasterio.crs import CRS
from shapely.geometry.base import BaseGeometry

import mapchete
from mapchete.commands.observer import ObserverProtocol, Observers
from mapchete.commands.parser import InputInfo
from mapchete.config import MapcheteConfig
from mapchete.config.parse import bounds_from_opts, raw_conf
from mapchete.enums import InputType
from mapchete.index import zoom_index_gen
from mapchete.types import MPathLike, Progress

logger = logging.getLogger(__name__)


def index(
    some_input: Union[MPathLike, dict, MapcheteConfig],
    idx_out_dir: Optional[MPathLike] = None,
    geojson: bool = False,
    gpkg: bool = False,
    shp: bool = False,
    fgb: bool = False,
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
    **_,
):
    """
    Create one or more indexes from a TileDirectory.
    """
    if not any([geojson, gpkg, shp, fgb, txt, vrt]):
        raise ValueError(
            """At least one of '--geojson', '--gpkg', '--shp', '--fgb', '--vrt' or '--txt'"""
            """must be provided."""
        )

    all_observers = Observers(observers)

    all_observers.notify(message=f"create index(es) for {some_input}")

    input_info = InputInfo.from_inp(some_input)
    if tile:
        tile = input_info.output_pyramid.tile(*tile)
        bounds = tile.bounds
        zoom = tile.zoom
    elif input_info.input_type == InputType.mapchete:
        try:
            bounds = bounds_from_opts(
                point=point,
                point_crs=point_crs,
                bounds=bounds,
                bounds_crs=bounds_crs,
                raw_conf=raw_conf(some_input),
            )
        except ValueError:
            pass
    else:
        bounds = bounds or input_info.bounds

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
                flatgeobuf=fgb,
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
