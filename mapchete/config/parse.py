import operator
import os
import warnings
from collections import OrderedDict
from typing import Any, Iterable, Optional, Tuple, Union

from rasterio.crs import CRS
from shapely import wkt
from shapely.geometry import Point, box, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from mapchete.config.models import ProcessConfig
from mapchete.errors import GeometryTypeError, MapcheteConfigError
from mapchete.io.vector import clean_geometry_type, fiona_open, reproject_geometry
from mapchete.path import MPath
from mapchete.tile import BufferedTilePyramid
from mapchete.types import Bounds, MPathLike, ZoomLevels, ZoomLevelsLike
from mapchete.validate import validate_values

_RESERVED_PARAMETERS = tuple(ProcessConfig.model_fields.keys())


def parse_config(
    input_config: Union[dict, MPathLike], strict: bool = False
) -> ProcessConfig:
    """Read config from file or dictionary and return validated configuration"""

    def _include_env(d: dict) -> OrderedDict:
        """Search for environment variables and add their values."""
        out = OrderedDict()
        for k, v in d.items():
            if isinstance(v, dict):
                out[k] = _include_env(v)
            elif isinstance(v, str) and v.startswith("${") and v.endswith("}"):
                envvar = v.lstrip("${").rstrip("}")
                out[k] = os.environ.get(envvar)
            else:
                out[k] = v
        return out

    def _config_to_dict(input_config: Union[dict, MPathLike]) -> dict:
        """Convert a file or dictionary to a config dictionary."""
        if isinstance(input_config, dict):
            if "config_dir" not in input_config:
                raise MapcheteConfigError("config_dir parameter missing")
            return OrderedDict(_include_env(input_config), mapchete_file=None)
        # from Mapchete file
        elif input_config.suffix == ".mapchete":
            config_dict = _include_env(input_config.read_yaml())
            return OrderedDict(
                config_dict,
                config_dir=config_dict.get(
                    "config_dir", input_config.absolute_path().dirname or os.getcwd()
                ),
                mapchete_file=input_config,
            )
        # throw error if unknown object
        else:  # pragma: no cover
            raise MapcheteConfigError(
                "Configuration has to be a dictionary or a .mapchete file."
            )

    if strict:  # pragma: no cover
        return ProcessConfig(**_config_to_dict(input_config))
    else:
        return ProcessConfig(**map_to_new_config_dict(_config_to_dict(input_config)))


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
        return map_to_new_config_dict(mapchete_file)
    else:
        return map_to_new_config_dict(MPath.from_inp(mapchete_file).read_yaml())


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
    params_per_zoom = OrderedDict()
    for zoom in zooms:
        params = OrderedDict()
        for name, element in config.model_dump().items():
            out_element = element_at_zoom(name, element, zoom)
            if out_element is not None:
                params[name] = out_element
        params_per_zoom[zoom] = params
    return OrderedDict(params_per_zoom)


def element_at_zoom(name: str, element: Any, zoom: int) -> Any:
    """
    Return the element filtered by zoom level.

    - An input integer or float gets returned as is.
    - An input string is checked whether it starts with "zoom". Then, the
      provided zoom level gets parsed and compared with the actual zoom
      level. If zoom levels match, the element gets returned.
    TODOs/gotchas:
    - Provided zoom levels for one element in config file are not allowed
      to "overlap", i.e. there is not yet a decision mechanism implemented
      which handles this case.
    """
    # If element is a dictionary, analyze subitems.
    if isinstance(element, dict):
        # we have an input or output driver here
        if "format" in element:
            return element

        # iterate through sub elements
        out_elements = OrderedDict()
        for sub_name, sub_element in element.items():
            out_element = element_at_zoom(sub_name, sub_element, zoom)
            if name in ["input", "process_parameters"] or out_element is not None:
                out_elements[sub_name] = out_element

        # If there is only one subelement, collapse unless it is
        # input. In such case, return a dictionary.
        if name not in ["input", "process_parameters"] and len(out_elements) == 1:
            return next(iter(out_elements.values()))

        # If subelement is empty, return None
        if len(out_elements) == 0:
            return None

        return out_elements

    # If element is a zoom level statement, filter element.
    elif isinstance(name, str):
        # filter out according to zoom filter definition
        if name.startswith("zoom"):
            return filter_by_zoom(
                conf_string=name.strip("zoom").strip(), zoom=zoom, element=element
            )

        # If element is a string but not a zoom level statement, return
        # element.
        else:
            return element

    # Return all other types as they are.
    else:  # pragma: no cover
        return element


def filter_by_zoom(
    element: Any = None, conf_string: str = None, zoom: int = None
) -> Any:
    """Return element only if zoom condition matches with config string."""

    def _strip_zoom(input_string: str, strip_string: str) -> int:
        """Return zoom level as integer or throw error."""
        try:
            return int(input_string.strip(strip_string))
        except Exception as e:
            raise MapcheteConfigError("zoom level could not be determined: %s" % e)

    for op_str, op_func in [
        # order of operators is important:
        # prematurely return in cases of "<=" or ">=", otherwise
        # _strip_zoom() cannot parse config strings starting with "<"
        # or ">"
        ("=", operator.eq),
        ("<=", operator.le),
        (">=", operator.ge),
        ("<", operator.lt),
        (">", operator.gt),
    ]:
        if conf_string.startswith(op_str):
            return element if op_func(zoom, _strip_zoom(conf_string, op_str)) else None


def bounds_from_opts(
    wkt_geometry: Optional[str] = None,
    point: Optional[Iterable[float]] = None,
    point_crs: Optional[CRS] = None,
    zoom: Optional[int] = None,
    bounds: Optional[Bounds] = None,
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
    BufferedTilePyramid
    """
    if wkt_geometry:
        return Bounds(*wkt.loads(wkt_geometry).bounds)
    elif point:
        x, y = point
        tp = raw_conf_process_pyramid(raw_conf)
        if point_crs:
            reproj = reproject_geometry(Point(x, y), src_crs=point_crs, dst_crs=tp.crs)
            x = reproj.x
            y = reproj.y
        zoom_levels = get_zoom_levels(
            process_zoom_levels=raw_conf["zoom_levels"], init_zoom_levels=zoom
        )
        return Bounds(
            *tp.without_pixelbuffer().tile_from_xy(x, y, max(zoom_levels)).bounds
        )
    elif bounds:
        bounds = Bounds.from_inp(bounds)
        if bounds_crs:
            tp = raw_conf_process_pyramid(raw_conf)
            bounds = Bounds(
                *reproject_geometry(
                    box(*bounds), src_crs=bounds_crs, dst_crs=tp.crs
                ).bounds
            )
        return bounds
    else:
        return


def get_zoom_levels(
    process_zoom_levels: ZoomLevelsLike = None,
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
        if str(some_input).upper().startswith(("POLYGON ", "MULTIPOLYGON ")):
            geom = wkt.loads(some_input)
        else:
            path = MPath.from_inp(some_input)
            with path.fio_env():
                with fiona_open(str(path.absolute_path(base_dir))) as src:
                    geom = unary_union([shape(f["geometry"]) for f in src])
                    crs = src.crs
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
    try:
        geom = clean_geometry_type(geom, "Polygon", allow_multipart=True)
    except GeometryTypeError:
        raise GeometryTypeError(
            f"area must either be a Polygon or a MultiPolygon, not {geom.geom_type}"
        )
    return geom, crs


def map_to_new_config_dict(config: dict) -> dict:
    """Takes an older style configuration and tries to convert it to a current version."""
    try:
        validate_values(config, [("output", dict)])
    except Exception as e:
        raise MapcheteConfigError(e)

    if "type" in config["output"]:  # pragma: no cover
        warnings.warn(DeprecationWarning("'type' is deprecated and should be 'grid'"))
        if "grid" not in config["output"]:
            config["output"]["grid"] = config["output"].pop("type")

    if "pyramid" not in config:
        warnings.warn(
            DeprecationWarning("'pyramid' needs to be defined in root config element.")
        )
        config["pyramid"] = dict(
            grid=config["output"]["grid"],
            metatiling=config.get("metatiling", 1),
            pixelbuffer=config.get("pixelbuffer", 0),
        )

    if "zoom_levels" not in config:
        warnings.warn(
            DeprecationWarning(
                "use new config element 'zoom_levels' instead of 'process_zoom', "
                "'process_minzoom' and 'process_maxzoom'"
            )
        )
        if "process_zoom" in config:
            config["zoom_levels"] = config["process_zoom"]
        elif all([i in config for i in ["process_minzoom", "process_maxzoom"]]):
            config["zoom_levels"] = dict(
                min=config["process_minzoom"], max=config["process_maxzoom"]
            )
        else:
            raise MapcheteConfigError("process zoom levels not provided in config")

    if "bounds" not in config:
        if "process_bounds" in config:
            warnings.warn(
                DeprecationWarning(
                    "'process_bounds' are deprecated and renamed to 'bounds'"
                )
            )
            config["bounds"] = config["process_bounds"]
        else:
            config["bounds"] = None

    if "input" not in config:
        if "input_files" in config:
            warnings.warn(
                DeprecationWarning(
                    "'input_files' are deprecated and renamed to 'input'"
                )
            )
            config["input"] = config["input_files"]
        else:
            raise MapcheteConfigError("no 'input' found")

    elif "input_files" in config:
        raise MapcheteConfigError(
            "'input' and 'input_files' are not allowed at the same time"
        )

    if "process_file" in config:
        warnings.warn(
            DeprecationWarning("'process_file' is deprecated and renamed to 'process'")
        )
        config["process"] = config.pop("process_file")

    process_parameters = config.get("process_parameters", {})
    for key in list(config.keys()):
        if key in _RESERVED_PARAMETERS:
            continue
        warnings.warn(
            "it puts the process parameter in the 'process_parameters' section, or it gets the warning again"
        )
        process_parameters[key] = config.pop(key)
    config["process_parameters"] = process_parameters

    return config
