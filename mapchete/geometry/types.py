from typing import (
    Type,
    Union,
)

from shapely.geometry import (
    GeometryCollection,
    LinearRing,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)

from mapchete.errors import GeometryTypeError
from mapchete.types import Geometry, MultipartGeometry, SinglepartGeometry


def get_multipart_type(geometry_type: Union[Type[Geometry], str]) -> MultipartGeometry:
    try:
        return {
            Point: MultiPoint,
            LineString: MultiLineString,
            Polygon: MultiPolygon,
            MultiPoint: MultiPoint,
            MultiLineString: MultiLineString,
            MultiPolygon: MultiPolygon,
        }[get_geometry_type(geometry_type)]  # type: ignore
    except KeyError:
        raise GeometryTypeError(
            f"geometry type {geometry_type} has no corresponding multipart type"
        )


def get_singlepart_type(
    geometry_type: Union[Type[Geometry], str],
) -> Type[SinglepartGeometry]:
    try:
        return {
            Point: Point,
            LineString: LineString,
            Polygon: Polygon,
            MultiPoint: Point,
            MultiLineString: LineString,
            MultiPolygon: Polygon,
        }[get_geometry_type(geometry_type)]  # type: ignore
    except KeyError:  # pragma: no cover
        raise GeometryTypeError(
            f"geometry type {geometry_type} has no corresponding multipart type"
        )


def get_geometry_type(
    geometry_type: Union[Type[Geometry], str, dict, Geometry],
) -> Type[Geometry]:
    if isinstance(geometry_type, str):
        try:
            return {
                "Point".lower(): Point,
                "LineString".lower(): LineString,
                "LinearRing".lower(): LinearRing,
                "Polygon".lower(): Polygon,
                "MultiPoint".lower(): MultiPoint,
                "MultiLineString".lower(): MultiLineString,
                "MultiPolygon".lower(): MultiPolygon,
                "GeometryCollection".lower(): GeometryCollection,
            }[geometry_type.lower()]
        except KeyError:
            raise GeometryTypeError(
                f"geometry type cannot be determined from {geometry_type}"
            )
    elif issubclass(geometry_type, Geometry):
        return geometry_type  # type: ignore
    raise GeometryTypeError(f"geometry type cannot be determined from {geometry_type}")
