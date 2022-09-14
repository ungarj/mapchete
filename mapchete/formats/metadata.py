"""
Functions handling output formats.

This module deserves a cleaner rewrite some day.
"""

import datetime
import dateutil.parser
import logging
import os
from pprint import pformat
from rasterio.crs import CRS
from typing import Any, Dict, Optional
import warnings

from mapchete.errors import MapcheteConfigError
from mapchete.io import read_json, write_json
from mapchete.tile import BufferedTilePyramid


logger = logging.getLogger(__name__)


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

    strategies = [
        ((datetime.date, datetime.datetime), _datetime_to_str),
    ]
    return _unparse_dict(out, strategies=strategies) if parse_datetime else out


def read_output_metadata(metadata_json: str, **kwargs: str) -> Dict:
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

    return load_metadata(read_json(metadata_json, **kwargs))


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
    if grid["type"] == "geodetic" and grid["shape"] == [2, 1]:  # pragma: no cover
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
        metadata_path = os.path.join(output_params["path"], "metadata.json")
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
            write_json(metadata_path, dump_params)


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
