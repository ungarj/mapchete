from typing import Generator, Union

from mapchete.errors import GeometryTypeError
from mapchete.geometry.types import (
    Geometry,
    GeometryCollection,
    MultipartGeometry,
    SinglepartGeometry,
    get_geometry_type,
    get_multipart_type,
)


def multipart_to_singleparts(
    geometry: Geometry,
) -> Generator[SinglepartGeometry, None, None]:
    """
    Yield single part geometries if geom is multipart, otherwise yield geom.

    Parameters
    ----------
    geom : shapely geometry

    Returns
    -------
    shapely single part geometries
    """
    if isinstance(geometry, GeometryCollection):
        for subgeom in geometry.geoms:
            yield from multipart_to_singleparts(subgeom)
    if isinstance(geometry, MultipartGeometry):
        for subgeom in geometry.geoms:
            yield subgeom
    elif isinstance(geometry, SinglepartGeometry):
        yield geometry
    else:  # pragma: no cover
        raise GeometryTypeError(f"invalid geometry type: {repr(geometry)}")


def is_type(
    geometry: Geometry,
    target_type: Union[str, Geometry],
    allow_multipart: bool = True,
) -> bool:
    target_type = get_geometry_type(target_type)
    if isinstance(geometry, target_type):
        return True
    elif isinstance(geometry, GeometryCollection):
        return False

    if allow_multipart:
        return isinstance(geometry, get_multipart_type(target_type))
    return False


def filter_by_geometry_type(
    geometry: Geometry,
    target_type: Union[str, Geometry],
    allow_multipart: bool = True,
):
    """Yields geometries only if they match the target type.

    When the input geometry is a multipart geometry
    If allow_multipart is set to False, multipart geometries are broken down into their
    subgeometries. If set to True, a MultiPoint will be yielded if the
    """
    target_type = get_geometry_type(target_type)
    if is_type(geometry, target_type=target_type, allow_multipart=allow_multipart):
        yield geometry

    elif isinstance(geometry, MultipartGeometry):
        for subgeometry in multipart_to_singleparts(geometry):
            yield from filter_by_geometry_type(
                subgeometry, target_type=target_type, allow_multipart=allow_multipart
            )
