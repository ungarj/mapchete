"""Convenience validator functions for core and extension packages."""

import warnings
from typing import Any, Dict, List, Union

import numpy.ma as ma
from rasterio.crs import CRS
from rasterio.profiles import Profile

from mapchete.protocols import GridProtocol
from mapchete.tile import BufferedTile, BufferedTilePyramid
from mapchete.bounds import Bounds
from mapchete.types import CRSLike
from mapchete.zoom_levels import ZoomLevels

########################
# validator functionrs #
########################


def validate_zooms(
    zooms: Union[int, Dict[str, int], List[int]], **kwargs
) -> ZoomLevels:
    """
    Return a list of zoom levels.

    Following inputs are converted:
    - int --> [int]
    - dict{min, max} --> [min ... max + 1]
    - [int] --> [int]
    - [int, int] --> [smaller int, bigger int + 1]

    Parameters
    ----------
    zoom : dict, int or list

    Returns
    -------
    List of zoom levels.
    """
    warnings.warn(
        DeprecationWarning(
            "'validate_zooms()' is deprecated and replaced 'ZoomLevels.from_inp()'"
        )
    )
    return ZoomLevels.from_inp(zooms)


def validate_bounds(bounds: Any) -> Bounds:
    """
    Return validated bounds.

    Bounds must be a list or tuple with exactly four elements.

    Parameters
    ----------
    bounds : list or tuple

    Returns
    -------
    Bounds

    Raises
    ------
    TypeError if type is invalid.
    """
    warnings.warn(
        DeprecationWarning(
            "'validate_bounds()' is deprecated and replaced 'Bounds.from_inp()'"
        )
    )
    return Bounds.from_inp(bounds)


def validate_values(config, values):
    """
    Return True if all values are given and have the desired type.

    Parameters
    ----------
    config : dict
        configuration dictionary
    values : list
        list of (str, type) tuples of values and value types expected in config

    Returns
    -------
    True if config is valid.

    Raises
    ------
    Exception if value is not found or has the wrong type.
    """
    if not isinstance(config, dict):
        raise TypeError("config must be a dictionary")
    for value, vtype in values:
        if value not in config:
            raise ValueError("%s not given" % value)
        if not isinstance(config[value], vtype):
            if config[value] is None:
                raise ValueError("%s not given" % value)
            raise TypeError(
                "%s must be %s, not %s" % (value, vtype, config[value])
            )  # pragma: no cover
    return True


def validate_tile(tile, pyramid):
    """
    Return BufferedTile object.

    Parameters
    ----------
    tile : tuple or BufferedTile
    pyramid : BufferedTilePyramid
        pyramid tile is being generated from if tile is tuple

    Returns
    -------
    BufferedTile

    Raises
    ------
    TypeError if tile type is invalid.
    """
    if isinstance(tile, tuple):
        return pyramid.tile(*tile)
    elif isinstance(tile, BufferedTile):
        return tile
    else:
        raise TypeError("tile must be tuple or BufferedTile: %s" % tile)


def validate_bufferedtilepyramid(pyramid):
    """
    Return BufferedTilePyramid.

    Parameters
    ----------
    pyramid : BufferedTilePyramid

    Returns
    -------
    BufferedTilePyramid

    Raises
    ------
    TypeError if type is invalid.
    """
    if isinstance(pyramid, BufferedTilePyramid):
        return pyramid
    else:
        raise TypeError("pyramid must be BufferedTilePyramid")


def validate_crs(crs: CRSLike) -> CRS:
    """
    Return crs as rasterio.crs.CRS.

    Parameters
    ----------
    crs : rasterio.crs.CRS, str, int or dict

    Returns
    -------
    rasterio.crs.CRS

    Raises
    ------
    TypeError if type is invalid.
    """
    if isinstance(crs, CRS):
        return crs
    elif isinstance(crs, str):
        try:
            return CRS().from_epsg(int(crs))
        except Exception:
            return CRS().from_string(crs)
    elif isinstance(crs, int):
        return CRS().from_epsg(crs)
    else:
        try:
            return CRS.from_user_input(crs)
        except Exception:
            raise TypeError(f"invalid CRS given: {crs}")


def validate_write_window_params(
    in_grid: GridProtocol,
    out_grid: GridProtocol,
    in_data: ma.MaskedArray,
    out_profile: Union[dict, Profile],
):
    """Raise Exception if write window parameters are invalid."""
    if any([not isinstance(t, GridProtocol) for t in [in_grid, out_grid]]):
        raise TypeError("in_grid and out_grid must implement GridProtocol")
    if not isinstance(in_data, ma.MaskedArray):
        raise TypeError("in_data must be ma.MaskedArray")
    if not isinstance(out_profile, dict):
        raise TypeError("out_profile must be a dictionary")


##############
# decorators #
##############


def deprecated_kwargs(func):
    """Decorator for open() functions warning of keyword argument usage."""

    def func_wrapper(*args, **kwargs):
        if kwargs:
            warnings.warn(
                "Using kwargs such in open() is deprecated and will have no effect."
            )
        if "resampling" in kwargs:
            raise DeprecationWarning(
                "'resampling' argument has no effect here and must be provided in read() "
                "function."
            )
        return func(*args, **kwargs)

    return func_wrapper
