from functools import partial
from typing import Union

from mapchete.errors import GeometryTypeError
from mapchete.geometry.transform import custom_transform
from mapchete.types import (
    CoordArrays,
    Geometry,
    LinearRing,
    LineString,
    MultiPolygon,
    Polygon,
)
from mapchete.bounds import Bounds


def segmentize_geometry(
    geometry: Union[Polygon, LinearRing, LineString, MultiPolygon],
    segmentize_value: float,
) -> Geometry:
    """
    Segmentize Polygon outer ring by segmentize value.

    Just Polygon geometry type supported.

    Parameters
    ----------
    geometry : ``shapely.geometry``
    segmentize_value: float

    Returns
    -------
    geometry : ``shapely.geometry``
    """
    if isinstance(geometry, (Polygon, LinearRing, LineString, MultiPolygon)):
        return custom_transform(
            geometry, partial(coords_segmentize, segmentize_value=segmentize_value)
        )
    else:
        raise GeometryTypeError(
            f"segmentize geometry must be a Polygon, LineaRing, Linestring or MultiPolygon: {repr(geometry)}"
        )


def coords_segmentize(coords: CoordArrays, segmentize_value: float) -> CoordArrays:
    out_x = []
    out_y = []
    x_coords, y_coords = map(list, coords)
    for points in zip(
        zip(x_coords[:-1], y_coords[:-1]), zip(x_coords[1:], y_coords[1:])
    ):
        line = LineString(points)
        for point in [
            line.interpolate(segmentize_value * i).coords[0]
            for i in range(int(line.length / segmentize_value))
        ] + [line.coords[1]]:
            out_x.append(point[0])
            out_y.append(point[1])
    return (out_x, out_y)


def get_segmentize_value(geometry: Geometry, segmentize_fraction: float) -> float:
    """Divide the smaller one of geometry height or width by segmentize fraction."""
    bounds = Bounds.from_inp(geometry.bounds, strict=False)
    return min([bounds.height, bounds.width]) / segmentize_fraction
