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
    else:  # pragma: no cover
        raise GeometryTypeError(f"invalid geometry: {geometry}")


def omit_empty_geometries(geometry: Geometry) -> Generator[Geometry, None, None]:
    if not geometry.is_empty:
        yield geometry


def is_type(
    geometry: Geometry,
    target_type: Union[GeometryTypeLike, Tuple[GeometryTypeLike, ...]],
    singlepart_equivalent_matches: bool = True,
    multipart_equivalent_matches: bool = True,
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
                    singlepart_equivalent_matches=singlepart_equivalent_matches,
                )
                for geom_type in target_type
            ]
        )

    geometry_type = get_geometry_type(geometry.geom_type)

    # simple match
    if geometry_type == get_geometry_type(target_type):
        return True

    # GeometryCollections don't have a corresponding singlepart or multipart type
    elif geometry_type == GeometryCollection:
        return False

    # a multi-part geometry matches its single-part relative
    elif (
        singlepart_equivalent_matches
        and get_singlepart_type(geometry_type) == target_type
    ):
        return True

    # a single-part geometry matches its multi-part relative
    elif (
        multipart_equivalent_matches
        and get_multipart_type(geometry_type) == target_type
    ):
        return True

    return False


def filter_by_geometry_type(
    geometry: Geometry,
    target_type: Union[GeometryTypeLike, Tuple[GeometryTypeLike]],
    singlepart_equivalent_matches: bool = True,
    multipart_equivalent_matches: bool = True,
    resolve_multipart_geometry: bool = True,
    resolve_geometrycollection: bool = True,
) -> Generator[Geometry, None, None]:
    """
    Yields geometries only if they match the target type.
    """
    if is_type(
        geometry,
        target_type=target_type,
        singlepart_equivalent_matches=singlepart_equivalent_matches,
        multipart_equivalent_matches=multipart_equivalent_matches,
    ):
        yield geometry

    elif resolve_geometrycollection and isinstance(geometry, GeometryCollection):
        for subgeometry in geometry.geoms:
            yield from filter_by_geometry_type(
                subgeometry,
                target_type=target_type,
                singlepart_equivalent_matches=singlepart_equivalent_matches,
                multipart_equivalent_matches=multipart_equivalent_matches,
                resolve_multipart_geometry=False,
            )

    elif resolve_multipart_geometry and isinstance(geometry, MultipartGeometry):
        for subgeometry in multipart_to_singleparts(geometry):
            yield from filter_by_geometry_type(
                subgeometry,
                target_type=target_type,
                singlepart_equivalent_matches=singlepart_equivalent_matches,
                multipart_equivalent_matches=multipart_equivalent_matches,
            )
