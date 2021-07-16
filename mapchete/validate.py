"""Convenience validator functions for core and extension packages."""


import numpy.ma as ma
from rasterio.crs import CRS
from tilematrix._funcs import Bounds
import warnings

from mapchete.tile import BufferedTile, BufferedTilePyramid

########################
# validator functionrs #
########################


def validate_zooms(zooms, expand=True):
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
    expand : bool
        Return full list of zoom levels instead of [min, max]

    Returns
    -------
    List of zoom levels.
    """
    if isinstance(zooms, dict):
        if any([a not in zooms for a in ["min", "max"]]):
            raise TypeError("min and max zoom required: %s" % str(zooms))
        zmin = validate_zoom(zooms["min"])
        zmax = validate_zoom(zooms["max"])
        if zmin > zmax:
            raise TypeError(
                "max zoom must not be smaller than min zoom: %s" % str(zooms)
            )
        return list(range(zmin, zmax + 1)) if expand else zooms
    elif isinstance(zooms, list):
        if len(zooms) == 1:
            return zooms
        elif len(zooms) == 2:
            zmin, zmax = sorted([validate_zoom(z) for z in zooms])
            return list(range(zmin, zmax + 1)) if expand else zooms
        else:
            return zooms
    else:
        return [validate_zoom(zooms)] if expand else zooms


def validate_zoom(zoom):
    """
    Return validated zoom.

    Assert zoom value is positive integer.

    Returns
    -------
    zoom

    Raises
    ------
    TypeError if type is invalid.
    """
    if any([not isinstance(zoom, int), zoom < 0]):
        raise TypeError("zoom must be a positive integer: %s" % zoom)
    return zoom


def validate_bounds(bounds):
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
    if not isinstance(bounds, (tuple, list)):
        raise TypeError("bounds must be either a tuple or a list: %s" % str(bounds))
    if len(bounds) != 4:
        raise ValueError("bounds has to have exactly four values: %s" % str(bounds))
    return Bounds(*bounds)


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
            raise TypeError("%s must be %s, not %s" % (value, vtype, config[value]))
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


def validate_crs(crs):
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
    elif isinstance(crs, dict):
        return CRS().from_dict(crs)
    else:
        raise TypeError("invalid CRS given")


def validate_write_window_params(in_tile, out_tile, in_data, out_profile):
    """Raise Exception if write window parameters are invalid."""
    if any([not isinstance(t, BufferedTile) for t in [in_tile, out_tile]]):
        raise TypeError("in_tile and out_tile must be BufferedTile")
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
