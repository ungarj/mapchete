from typing import Any, Optional

from mapchete._deprecated import deprecated
from mapchete.io.vector.convert import convert_vector
from mapchete.io.vector.indexed_features import (
    IndexedFeatures,
    read_vector,
    read_union_geometry,
)
from mapchete.io.vector.open import fiona_open
from mapchete.io.vector.read import (
    fiona_read,
    read_vector_window,
)
from mapchete.io.vector.write import fiona_write, write_vector_window
from mapchete.types import Geometry, GeometryLike, CRSLike, BoundsLike

__all__ = [
    "fiona_read",
    "fiona_write",
    "fiona_open",
    "read_vector_window",
    "write_vector_window",
    "IndexedFeatures",
    "convert_vector",
    "read_vector",
    "read_union_geometry",
]


@deprecated(
    reason="mapchete.vector.io.to_shape has moved to mapchete.geometry.to_shape"
)
def to_shape(geometry: Any) -> Geometry:  # pragma: no cover
    from mapchete.geometry import to_shape

    return to_shape(geometry)


@deprecated(
    reason="mapchete.vector.io.reproject_geometry has moved to mapchete.geometry.reproject_geometry"
)
def reproject_geometry(
    geometry: GeometryLike,
    src_crs: CRSLike,
    dst_crs: CRSLike,
    clip_to_crs_bounds: bool = True,
    error_on_clip: bool = False,
    segmentize_on_clip: bool = False,
    segmentize: bool = False,
    segmentize_fraction: float = 100.0,
    validity_check: bool = True,
    antimeridian_cutting: bool = False,
    retry_with_clip: bool = True,
    fiona_env: Optional[dict] = None,
) -> Geometry:  # pragma: no cover
    from mapchete.geometry import reproject_geometry

    return reproject_geometry(
        geometry=geometry,
        src_crs=src_crs,
        dst_crs=dst_crs,
        clip_to_crs_bounds=clip_to_crs_bounds,
        error_on_clip=error_on_clip,
        segmentize_on_clip=segmentize_on_clip,
        segmentize=segmentize,
        segmentize_fraction=segmentize_fraction,
        validity_check=validity_check,
        antimeridian_cutting=antimeridian_cutting,
        retry_with_clip=retry_with_clip,
        fiona_env=fiona_env,
    )


@deprecated(
    reason="mapchete.io.vector.bounds_intersect was removed, use the Bounds.intersect(other) method instead."
)
def bounds_intersect(
    bounds1: BoundsLike, bounds2: BoundsLike
) -> bool:  # pragma: no cover
    from mapchete.bounds import bounds_intersect

    return bounds_intersect(bounds1, bounds2)
