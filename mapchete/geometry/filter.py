from typing import Generator, Tuple, Union

from mapchete.errors import GeometryTypeError
from mapchete.geometry.types import (
    get_geometry_type,
    get_multipart_type,
    GeometryTypeLike,
    get_singlepart_type,
)
from mapchete.types import (
    Geometry,
    GeometryCollection,
    MultipartGeometry,
    SinglepartGeometry,
)


def multipart_to_singleparts(
    geometry: Geometry,
) -> Generator[SinglepartGeometry, None, None]:
    """Yields all singlepart subgeometries."""
    if isinstance(geometry, MultipartGeometry):
        for subgeometry in geometry.geoms:
            yield from multipart_to_singleparts(subgeometry)
    elif isinstance(geometry, SinglepartGeometry):
        yield geometry
    else:
        raise GeometryTypeError(f"invalid geometry: {geometry}")


def omit_empty_geometries(geometry: Geometry) -> Generator[Geometry, None, None]:
    if not geometry.is_empty:
        yield geometry


def is_type(
    geometry: Geometry,
    target_type: Union[GeometryTypeLike, Tuple[GeometryTypeLike]],
    singlepart_matches_multipart: bool = True,
    multipart_matches_singlepart: bool = True,
) -> bool:
    """
    Checks whether geometry type is in alignment with target type.
    """
    if isinstance(target_type, tuple):
        return any(
            [
                is_type(
                    geometry,
                    target_type=geom_type,
                    singlepart_matches_multipart=singlepart_matches_multipart,
                )
                for geom_type in target_type
            ]
        )

    geometry_type = get_geometry_type(geometry.geom_type)

    if geometry_type == get_geometry_type(target_type):
        return True

    # a single-part geometry matches its multi-part relative
    elif (
        singlepart_matches_multipart
        and get_multipart_type(geometry_type) == target_type
    ):
        return True

    # a multi-part geometry matches its single-part relative
    elif (
        multipart_matches_singlepart
        and get_singlepart_type(geometry_type) == target_type
    ):
        return True

    return False


def filter_by_geometry_type(
    geometry: Geometry,
    target_type: Union[GeometryTypeLike, Tuple[GeometryTypeLike]],
    singlepart_matches_multipart: bool = True,
    multipart_matches_singlepart: bool = True,
    resolve_multipart_geometry: bool = True,
    resolve_geometrycollection: bool = True,
) -> Generator[Geometry, None, None]:
    """
    Yields geometries only if they match the target type.
    """
    if is_type(
        geometry,
        target_type=target_type,
        singlepart_matches_multipart=singlepart_matches_multipart,
        multipart_matches_singlepart=multipart_matches_singlepart,
    ):
        yield geometry

    elif resolve_geometrycollection and isinstance(geometry, GeometryCollection):
        for subgeometry in geometry.geoms:
            yield from filter_by_geometry_type(
                subgeometry,
                target_type=target_type,
                singlepart_matches_multipart=singlepart_matches_multipart,
                multipart_matches_singlepart=multipart_matches_singlepart,
                resolve_multipart_geometry=False,
            )

    elif resolve_multipart_geometry and isinstance(geometry, MultipartGeometry):
        for subgeometry in multipart_to_singleparts(geometry):
            yield from filter_by_geometry_type(
                subgeometry,
                target_type=target_type,
                singlepart_matches_multipart=singlepart_matches_multipart,
                multipart_matches_singlepart=multipart_matches_singlepart,
            )
