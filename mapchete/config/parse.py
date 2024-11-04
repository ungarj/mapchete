from collections import OrderedDict
from typing import Iterable, Optional, Tuple, Union

from rasterio.crs import CRS
from shapely import wkt
from shapely.geometry import Point, shape
from shapely.geometry.base import BaseGeometry

from mapchete.bounds import Bounds
from mapchete.config.models import ProcessConfig, ZoomParameters
from mapchete.errors import GeometryTypeError
from mapchete.geometry import is_type, reproject_geometry
from mapchete.geometry.types import Polygon, MultiPolygon
from mapchete.io.vector.indexed_features import IndexedFeatures
from mapchete.path import MPath
from mapchete.tile import BufferedTilePyramid
from mapchete.types import BoundsLike, MPathLike, ZoomLevelsLike
from mapchete.zoom_levels import ZoomLevels


def parse_config(
    input_config: Union[dict, MPathLike], strict: bool = False
) -> ProcessConfig:
    """Read config from file or dictionary and return validated configuration"""
    return ProcessConfig.parse(input_config, strict)


def raw_conf(mapchete_file: Union[dict, MPathLike]) -> dict:
    """
    Load a mapchete_file into a dictionary.

    Parameters
    ----------
    mapchete_file : str
        Path to a Mapchete file.

    Returns
    -------
    dictionary
    """
    if isinstance(mapchete_file, dict):
        return ProcessConfig.map_to_new_config_dict(mapchete_file)
    else:
        return ProcessConfig.map_to_new_config_dict(
            MPath.from_inp(mapchete_file).read_yaml()
        )


def raw_conf_process_pyramid(
    raw_conf: dict, reset_pixelbuffer: bool = False
) -> BufferedTilePyramid:
    """
    Load the process pyramid of a raw configuration.

    Parameters
    ----------
    raw_conf : dict
        Raw mapchete configuration as dictionary.

    Returns
    -------
    BufferedTilePyramid
    """
    pixelbuffer = 0 if reset_pixelbuffer else raw_conf["pyramid"].get("pixelbuffer", 0)
    return BufferedTilePyramid(
        raw_conf["pyramid"]["grid"],
        metatiling=raw_conf["pyramid"].get("metatiling", 1),
        pixelbuffer=pixelbuffer,
    )


def raw_conf_output_pyramid(raw_conf: dict) -> BufferedTilePyramid:
    """
    Load the process pyramid of a raw configuration.

    Parameters
    ----------
    raw_conf : dict
        Raw mapchete configuration as dictionary.

    Returns
    -------
    BufferedTilePyramid
    """
    return BufferedTilePyramid(
        raw_conf["pyramid"]["grid"],
        metatiling=raw_conf["output"].get(
            "metatiling", raw_conf["pyramid"].get("metatiling", 1)
        ),
        pixelbuffer=raw_conf["pyramid"].get(
            "pixelbuffer", raw_conf["pyramid"].get("pixelbuffer", 0)
        ),
    )


def raw_conf_at_zoom(config: ProcessConfig, zooms: ZoomLevels) -> OrderedDict:
    """Return parameter dictionary per zoom level."""
    return config.raw_conf_per_zooms(zooms)


def zoom_parameters(config: ProcessConfig, zoom: int) -> ZoomParameters:
    """Return parameter dictionary per zoom level."""
    return config.zoom_parameters(zoom)


def bounds_from_opts(
    wkt_geometry: Optional[str] = None,
    point: Optional[Iterable[float]] = None,
    point_crs: Optional[CRS] = None,
    zoom: Optional[int] = None,
    bounds: Optional[BoundsLike] = None,
    bounds_crs: Optional[CRS] = None,
    raw_conf: Optional[dict] = None,
) -> Bounds:
    """
    Return process bounds depending on given inputs.

    Parameters
    ----------
    wkt_geometry : string
        WKT geometry used to generate bounds.
    point : iterable
        x and y coordinates of point whose corresponding process tile bounds shall be
        returned.
    point_crs : str or CRS
        CRS of point (default: process pyramid CRS)
    zoom : int
        Mandatory zoom level if point is provided.
    bounds : iterable
        Bounding coordinates to be used
    bounds_crs : str or CRS
        CRS of bounds (default: process pyramid CRS)

    raw_conf : dict
        Raw mapchete configuration as dictionary.

    Returns
    -------
    Bounds
    """
    if wkt_geometry:
        return Bounds(*wkt.loads(wkt_geometry).bounds)
    elif point:
        if raw_conf:
            x, y = point
            tp = raw_conf_process_pyramid(raw_conf)
            if point_crs:
                reproj = reproject_geometry(
                    Point(x, y), src_crs=point_crs, dst_crs=tp.crs
                )
                x = reproj.x  # type: ignore
                y = reproj.y  # type: ignore
            zoom_levels = get_zoom_levels(
                process_zoom_levels=raw_conf["zoom_levels"], init_zoom_levels=zoom
            )
            return Bounds(
                *tp.without_pixelbuffer().tile_from_xy(x, y, max(zoom_levels)).bounds
            )
        raise ValueError("raw_conf is required")
    elif bounds:
        bounds = Bounds.from_inp(bounds)
        if bounds_crs:
            if raw_conf:
                tp = raw_conf_process_pyramid(raw_conf)
            else:
                raise ValueError("raw_conf is required")
            bounds = Bounds(
                *reproject_geometry(
                    shape(bounds), src_crs=bounds_crs, dst_crs=tp.crs
                ).bounds
            )
        return bounds

    raise ValueError("cannot determine bounds")


def get_zoom_levels(
    process_zoom_levels: ZoomLevelsLike,
    init_zoom_levels: Optional[ZoomLevelsLike] = None,
) -> ZoomLevels:
    """Validate and return zoom levels."""
    process_zoom_levels = ZoomLevels.from_inp(process_zoom_levels)
    if init_zoom_levels is None:
        return process_zoom_levels
    else:
        init_zoom_levels = ZoomLevels.from_inp(init_zoom_levels)
        if not set(init_zoom_levels).issubset(
            set(process_zoom_levels)
        ):  # pragma: no cover
            raise ValueError("init zooms must be a subset of process zoom")
        return init_zoom_levels


def guess_geometry(
    some_input: Union[MPathLike, dict, BaseGeometry], base_dir=None
) -> Tuple[BaseGeometry, CRS]:
    """
    Guess and parse geometry if possible.

    - a WKT string
    - a GeoJSON mapping
    - a shapely geometry
    - a path to a Fiona-readable file
    """
    crs = None
    # WKT or path:
    if isinstance(some_input, (str, MPath)):
        if (
            str(some_input)
            .upper()
            .startswith(
                (
                    "POINT ",
                    "MULTIPOINT ",
                    "LINESTRING ",
                    "MULTILINESTRING ",
                    "POLYGON ",
                    "MULTIPOLYGON ",
                    "GEOMETRYCOLLECTION ",
                )
            )
        ):
            geom = wkt.loads(str(some_input))
        else:
            features = IndexedFeatures.from_file(
                MPath.from_inp(some_input).absolute_path(base_dir)
            )
            geom = features.read_union_geometry()
            crs = features.crs
    # GeoJSON mapping
    elif isinstance(some_input, dict):
        geom = shape(some_input)
    # shapely geometry
    elif isinstance(some_input, BaseGeometry):
        geom = some_input
    else:
        raise TypeError(
            "area must be either WKT, GeoJSON mapping, shapely geometry or a "
            "Fiona-readable path."
        )
    if not geom.is_valid:  # pragma: no cover
        raise TypeError("area is not a valid geometry")
    if not is_type(geom, target_type=(Polygon, MultiPolygon)):
        raise GeometryTypeError(
            f"area must either be a Polygon or a MultiPolygon, not {geom.geom_type}"
        )
    return geom, crs
