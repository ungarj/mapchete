from typing import (
    Dict,
    Iterable,
    Literal,
    Protocol,
    Tuple,
    Type,
    Union,
    runtime_checkable,
)

from pydantic_geojson import GeometryType as GeoJSONGeometryType
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
from shapely.geometry.base import BaseGeometry

from mapchete.errors import GeometryTypeError

SinglepartGeometry = Union[
    Point,
    LineString,
    LinearRing,
    Polygon,
]

MultipartGeometry = Union[
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
]

Geometry = Union[SinglepartGeometry, MultipartGeometry, BaseGeometry]


@runtime_checkable
class GeoInterface(Protocol):
    __geo_interface__: Union[
        GeoJSONGeometryType, Dict[Literal["geometry"], GeoJSONGeometryType]
    ]


GeometryLike = Union[Geometry, GeoJSONGeometryType, GeoInterface]

CoordArrays = Tuple[Iterable[float], Iterable[float]]


def get_multipart_type(geometry: Geometry) -> MultipartGeometry:
    try:
        return {
            Point: MultiPoint,
            LineString: MultiLineString,
            Polygon: MultiPolygon,
            MultiPoint: MultiPoint,
            MultiLineString: MultiLineString,
            MultiPolygon: MultiPolygon,
        }[geometry]
    except KeyError:
        raise GeometryTypeError(
            f"geometry type {geometry.type} has no corresponding multipart type"
        )


def get_geometry_type(geometry_type: Union[Type[Geometry], str]) -> Type[Geometry]:
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
        return geometry_type
    raise GeometryTypeError(f"geometry type cannot be determined from {geometry_type}")
