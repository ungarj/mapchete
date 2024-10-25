from typing import (
    Dict,
    Type,
    Union,
)


from mapchete.errors import GeometryTypeError
from mapchete.types import (
    Geometry,
    MultipartGeometry,
    SinglepartGeometry,
    GeometryCollection,
    LinearRing,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)


TO_MULTIPART: Dict[Type[Geometry], Type[MultipartGeometry]] = {
    Point: MultiPoint,
    LineString: MultiLineString,
    Polygon: MultiPolygon,
    MultiPoint: MultiPoint,
    MultiLineString: MultiLineString,
    MultiPolygon: MultiPolygon,
    GeometryCollection: GeometryCollection,
}


TO_SINGLEPART: Dict[Type[Geometry], Type[SinglepartGeometry]] = {
    Point: Point,
    LineString: LineString,
    Polygon: Polygon,
    MultiPoint: Point,
    MultiLineString: LineString,
    MultiPolygon: Polygon,
}

GeometryTypeLike = Union[Type[Geometry], str]


def get_multipart_type(geometry_type: GeometryTypeLike) -> Type[MultipartGeometry]:
    try:
        return TO_MULTIPART[get_geometry_type(geometry_type)]
    except KeyError:  # pragma: no cover
        raise GeometryTypeError(
            f"geometry type {geometry_type} has no corresponding multipart type"
        )


def get_singlepart_type(
    geometry_type: GeometryTypeLike,
) -> Type[SinglepartGeometry]:
    try:
        return TO_SINGLEPART[get_geometry_type(geometry_type)]
    except KeyError:  # pragma: no cover
        raise GeometryTypeError(
            f"geometry type {geometry_type} has no corresponding singlepart type"
        )


def str_to_geometry_type(geometry_type: str) -> Type[Geometry]:
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


def get_geometry_type(
    geometry: GeometryTypeLike,
) -> Type[Geometry]:
    if isinstance(geometry, str):
        return str_to_geometry_type(geometry)
    try:
        if issubclass(geometry, Geometry):
            return geometry
    except TypeError:  # pragma: no cover
        pass
    raise GeometryTypeError(f"geometry type cannot be determined from {geometry}")
