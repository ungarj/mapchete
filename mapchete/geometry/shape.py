from shapely.geometry import shape

from mapchete.geometry.types import GeoInterface, Geometry, GeometryLike


def to_shape(geometry: GeometryLike) -> Geometry:
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
        elif (
            isinstance(geometry, GeoInterface)
            and isinstance(geometry.__geo_interface__, dict)
            and geometry.__geo_interface__.get("geometry")
        ):
            return shape(geometry.__geo_interface__["geometry"])
        else:
            return shape(geometry)
    except Exception:  # pragma: no cover
        raise TypeError(f"invalid geometry type: {type(geometry)}")
