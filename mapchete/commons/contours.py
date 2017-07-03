"""Contour line extraction using matplotlib."""

import matplotlib
# Must be called before pyplot otherwise Travis fails
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from shapely.geometry import LineString, mapping


def extract_contours(array, tile, interval=100, field='elev', base=0):
    """
    Extract contour lines from an array.

    Parameters
    ----------
    array : array
        input elevation data
    tile : Tile
        tile covering the array
    interval : integer
        elevation value interval when drawing contour lines
    field : string
        output field name containing elevation value
    base : integer
        elevation base value the intervals are computed from

    Returns
    -------
    contours : iterable
        contours as GeoJSON-like pairs of properties and geometry
    """
    levels = _get_contour_values(
        array.min(), array.max(), interval=interval, base=base)
    if not levels:
        return []
    contours = plt.contour(array, levels)
    index = 0
    out_contours = []
    for level in range(len(contours.collections)):
        elevation = levels[index]
        index += 1
        paths = contours.collections[level].get_paths()
        for path in paths:
            out_coords = [
                (
                    tile.left+(i[1]*tile.pixel_x_size),
                    tile.top-(i[0]*tile.pixel_y_size), )
                for i in zip(path.vertices[:, 1], path.vertices[:, 0])
            ]
            if len(out_coords) >= 2:
                out_contours.append(
                    dict(
                        properties={field: elevation},
                        geometry=mapping(LineString(out_coords))
                    )
                )
    return out_contours


def _get_contour_values(min_val, max_val, base=0, interval=100):
    """Return a list of values between min and max within an interval."""
    i = base
    out = []
    if min_val < base:
        while i >= min_val:
            i -= interval
    while i <= max_val:
        if i >= min_val:
            out.append(i)
        i += interval
    return out
