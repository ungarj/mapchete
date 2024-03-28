"""
Functions handling output formats.

This module deserves a cleaner rewrite some day.
"""

import datetime
import logging
import warnings
from pprint import pformat
from typing import Dict

import dateutil.parser
from rasterio.crs import CRS
from shapely.geometry.base import BaseGeometry

from mapchete.errors import MapcheteConfigError, MapcheteDriverError
from mapchete.io import MPath, fiona_open, rasterio_open
from mapchete.registered import drivers
from mapchete.tile import BufferedTilePyramid
from mapchete.types import MPathLike

logger = logging.getLogger(__name__)


def available_output_formats() -> Dict:
    """
    Return all available output formats.

    Returns
    -------
    formats : list
        all available output formats
    """
    output_formats = {}
    for v in drivers:
        driver_ = v.load()
        if hasattr(driver_, "METADATA") and (driver_.METADATA["mode"] in ["w", "rw"]):
            output_formats[driver_.METADATA["driver_name"]] = driver_.METADATA
    return output_formats


def available_input_formats() -> Dict:
    """
    Return all available input formats.

    Returns
    -------
    formats : list
        all available input formats
    """
    input_formats = {}
    for v in drivers:
        logger.debug("driver found: %s", v)
        driver_ = v.load()
        if hasattr(driver_, "METADATA") and (driver_.METADATA["mode"] in ["r", "rw"]):
            input_formats[driver_.METADATA["driver_name"]] = driver_.METADATA
    return input_formats


def driver_metadata(driver_name: str) -> Dict:
    """
    Return driver metadata.

    Parameters
    ----------
    driver_name : str
        Name of driver.

    Returns
    -------
    Driver metadata as dictionary.
    """
    for v in drivers:
        driver_ = v.load()
        if hasattr(driver_, "METADATA") and (
            driver_.METADATA["driver_name"] == driver_name
        ):
            return dict(driver_.METADATA)
    else:  # pragma: no cover
        raise ValueError(f"driver '{driver_name}' not found")


def driver_from_file(input_file: MPathLike, quick: bool = True) -> str:
    """
    Guess driver from file by opening it.

    Parameters
    ----------
    input_file : str
        Path to file.
    quick : bool
        Try to guess driver from known file extensions instead of trying to open with
        fiona and rasterio. (default: True)

    Returns
    -------
    driver : string
        driver name
    """
    input_file = MPath.from_inp(input_file)

    # mapchete files can immediately be returned:
    if input_file.suffix == ".mapchete":
        return "Mapchete"

    # use the most common file extensions to quickly determine input driver for file:
    if quick:
        try:
            return driver_from_extension(input_file.suffix)
        except ValueError:
            pass

    # brute force by trying to open file with rasterio and fiona:
    try:
        logger.debug("try to open %s with rasterio...", input_file)
        with rasterio_open(input_file):  # pragma: no cover
            return "raster_file"
    except Exception as rio_exception:
        try:
            logger.debug("try to open %s with fiona...", input_file)
            with fiona_open(input_file):  # pragma: no cover
                return "vector_file"
        except Exception as fio_exception:
            if input_file.exists():
                logger.exception(f"fiona error: {fio_exception}")
                logger.exception(f"rasterio error: {rio_exception}")
                raise MapcheteDriverError(
                    "%s has an unknown file extension and could not be opened by neither "
                    "rasterio nor fiona." % input_file
                )
            else:
                raise FileNotFoundError("%s does not exist" % input_file)


def driver_from_extension(file_extension: str) -> str:
    """
    Guess driver name from file extension.

    Paramters
    ---------
    file_extension : str
        File extension to look for.

    Returns
    -------
    driver : string
        driver name
    """
    file_extension = file_extension.lstrip(".")
    all_drivers_extensions = {}
    for v in drivers:
        driver = v.load()
        try:
            driver_extensions = driver.METADATA.get("file_extensions", []).copy()
            all_drivers_extensions[driver.METADATA["driver_name"]] = driver_extensions
            if driver_extensions and file_extension in driver_extensions:
                return driver.METADATA["driver_name"]
        except AttributeError:  # pragma: no cover
            pass
    else:
        raise ValueError(
            f"driver name for file extension {file_extension} could not be found: {all_drivers_extensions}"
        )


def data_type_from_extension(file_extension: str) -> str:
    """
    Guess data_type (raster or vector) from file extension.

    Paramters
    ---------
    file_extension : str
        File extension to look for.

    Returns
    -------
    driver data type : string
        driver data type
    """
    for v in drivers:
        driver = v.load()
        try:
            driver_extensions = driver.METADATA.get("file_extensions", [])
            if driver_extensions and file_extension in driver_extensions:
                return driver.METADATA["data_type"]
        except AttributeError:  # pragma: no cover
            pass
    else:
        raise ValueError(
            f"data type for file extension {file_extension} could not be found"
        )


def dump_metadata(params: Dict, parse_datetime=True) -> Dict:
    """
    Transform params to JSON-serializable dictionary for a metadata.json file.

    This also converts the BufferedTilePyramid to a dictionary and any datetime
    objects into strings.

    Parameters
    ----------
    params : dict
        Mapping of process parameters

    Returns
    -------
    Dictionary of output parameters ready to be written as metadata.json.
    """
    # in case GridDefinition was not yet initialized
    out = dict(
        pyramid=BufferedTilePyramid(
            grid=params["grid"],
            tile_size=params.get("tile_size", 256),
            metatiling=params.get("metatiling", 1),
            pixelbuffer=params.get("pixelbuffer", 0),
        ).to_dict(),
        driver={
            k: v
            for k, v in params.items()
            if k not in ["path", "grid", "pixelbuffer", "metatiling"]
        },
    )

    def _datetime_to_str(value):
        return value.isoformat()

    def _geometry_to_none(value):
        return None

    strategies = [
        ((datetime.date, datetime.datetime), _datetime_to_str),
        (MPath, str),
        (BaseGeometry, _geometry_to_none),
    ]
    return _unparse_dict(out, strategies=strategies) if parse_datetime else out


def read_output_metadata(metadata_json: MPathLike, **kwargs: str) -> Dict:
    """
    Read and parse metadata.json.

    Parameters
    ----------
    metadata_json : str
        Path to metadata.json file.

    Returns
    -------
        Parsed output metadata.
    """

    return load_metadata(MPath.from_inp(metadata_json).read_json())


def load_metadata(params: Dict, parse_known_types=True) -> Dict:
    """
    Parse output metadata dictionary.

    This function raises DeprecationWarning instances if needed and initializes the
    BufferedTilePyramid as well as datetime objects.

    Parameters
    ----------
    params : dict
        Output metadata parameters.

    Returns
    -------
    out_params : dict
        Output metadata parameters with initialized BufferedTilePyramid.
    """
    if not isinstance(params, dict):  # pragma: no cover
        raise TypeError(f"metadata parameters must be a dictionary, not {params}")
    out = params.copy()
    grid = out["pyramid"]["grid"]
    if "type" in grid:  # pragma: no cover
        warnings.warn(DeprecationWarning("'type' is deprecated and should be 'grid'"))
        if "grid" not in grid:
            grid["grid"] = grid.pop("type")

    if grid["grid"] == "geodetic" and grid["shape"] == [2, 1]:  # pragma: no cover
        warnings.warn(
            DeprecationWarning(
                "Deprecated grid shape ordering found. "
                "Please change grid shape from [2, 1] to [1, 2]."
            )
        )
        out["pyramid"]["grid"]["shape"] = [1, 2]
    if "crs" in grid and isinstance(grid["crs"], str):
        crs = CRS.from_string(grid["crs"])
        warnings.warn(
            DeprecationWarning(
                "Deprecated 'srs' found in: '%s'. "
                "Use WKT representation instead: %s"
                % (grid["crs"], pformat(dict(wkt=crs.to_wkt())))
            )
        )
        out["pyramid"]["grid"].update(srs=dict(wkt=crs.to_wkt()))

    out.update(
        pyramid=BufferedTilePyramid(
            out["pyramid"]["grid"],
            metatiling=out["pyramid"].get("metatiling", 1),
            tile_size=out["pyramid"].get("tile_size", 256),
            pixelbuffer=out["pyramid"].get("pixelbuffer", 0),
        )
    )

    strategies = [
        # create datetime objects and skip on these allowed exceptions
        (dateutil.parser.parse, (dateutil.parser.ParserError, TypeError)),
    ]
    return _parse_dict(out, strategies=strategies) if parse_known_types else out


def write_output_metadata(output_params: Dict) -> None:
    """
    Write output JSON and verify parameters if output metadata exist.

    Parameters
    ----------
    output_params : dict
        Output parameters
    """
    if "path" in output_params:
        metadata_path = MPath.from_inp(output_params) / "metadata.json"
        logger.debug("check for output %s", metadata_path)
        try:
            existing_params = read_output_metadata(metadata_path)
            logger.debug("%s exists", metadata_path)
            logger.debug("existing output parameters: %s", pformat(existing_params))
            current_params = dump_metadata(output_params)
            logger.debug("current output parameters: %s", pformat(current_params))
            compare_metadata_params(existing_params, current_params)
        except FileNotFoundError:
            logger.debug("%s does not exist", metadata_path)
            dump_params = dump_metadata(output_params)
            # dump output metadata
            metadata_path.write_json(dump_params)


def compare_metadata_params(params1: Dict, params2: Dict) -> None:
    """
    Verify that both mappings of output metadata parameters are compatible.

    Parameters
    ----------
    params1 : dict
        Output metadata parameters.
    params2 : dict
        Output metadata parameters.
    """

    def _buffered_pyramid(pyramid):
        if isinstance(pyramid, dict):
            return BufferedTilePyramid(**pyramid)
        else:
            return pyramid

    params1_tp = _buffered_pyramid(params1["pyramid"])
    params2_tp = _buffered_pyramid(params2["pyramid"])
    if params1_tp != params2_tp:  # pragma: no cover
        raise MapcheteConfigError(
            "pyramid definitions between existing and new output do not match: "
            "%s != %s" % (params1_tp, params2_tp)
        )
    if params1["driver"]["format"] != params2["driver"]["format"]:  # pragma: no cover
        raise MapcheteConfigError(
            "existing output format does not match new output format: "
            "%s != %s" % ((params1["driver"]["format"], params2["driver"]["format"]))
        )


def _parse_dict(d, strategies=None):
    """Iterate through dictionary and try to parse values according to strategies."""

    def _parse_val(val):
        for func, allowed_exception in strategies:
            try:
                return func(val)
            except allowed_exception:
                pass
        else:
            return val

    strategies = strategies or []
    out = dict()
    for k, v in d.items():
        if isinstance(v, dict):
            v = _parse_dict(v, strategies=strategies)
        elif isinstance(v, list):
            v = [_parse_val(val) for val in v]
        elif isinstance(v, tuple):
            v = tuple(_parse_val(val) for val in v)
        else:
            v = _parse_val(v)
        out[k] = v
    return out


def _unparse_dict(d, strategies=None):
    """Iterate through dictionary and try to unparse values according to strategies."""

    def _unparse_val(val):
        for instance_type, func in strategies:
            if isinstance(val, instance_type):
                return func(val)
        else:
            return val

    strategies = strategies or []
    out = dict()
    for k, v in d.items():
        if isinstance(v, dict):
            v = _unparse_dict(v, strategies=strategies)
        elif isinstance(v, list):
            v = [_unparse_val(val) for val in v]
        elif isinstance(v, tuple):
            v = tuple(_unparse_val(val) for val in v)
        else:
            v = _unparse_val(v)
        out[k] = v
    return out
