from typing import Any
from shapely.geometry import shape

from mapchete.types import GeoInterface, Geometry


def to_shape(geometry: Any) -> Geometry:
    """
    Convert geometry to shapely geometry if necessary.

    Parameters
    ----------
    geom : shapely geometry or GeoJSON mapping

    Returns
    -------
    shapely geometry
    """
    try:
        if isinstance(geometry, Geometry):
            return geometry
        elif isinstance(geometry, dict) and geometry.get("geometry"):
            return shape(geometry["geometry"])
        elif (
            isinstance(geometry, GeoInterface)
            and isinstance(geometry.__geo_interface__, dict)
            and geometry.__geo_interface__.get("geometry")
        ):
            return shape(geometry.__geo_interface__["geometry"])
        else:
            return shape(geometry)  # type: ignore
    except Exception:  # pragma: no cover
        raise TypeError(f"invalid geometry type: {type(geometry)}")
