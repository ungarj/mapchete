from typing import Callable

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

from mapchete.geometry.repair import repair
from mapchete.geometry.types import Geometry


def custom_transform(geometry: Geometry, func: Callable) -> Geometry:
    # todo: shapely.transform.transform maybe can make this code more simple
    # https://shapely.readthedocs.io/en/stable/reference/shapely.transform.html#shapely.transform
    def _point(point: Point) -> Point:
        return Point(zip(*func(point.xy)))

    def _multipoint(multipoint: MultiPoint) -> MultiPoint:
        return MultiPoint([_point(point) for point in multipoint.geoms])

    def _linestring(linestring: LineString) -> LineString:
        return LineString(zip(*func(linestring.xy)))

    def _multilinestring(multilinestring: MultiLineString) -> MultiLineString:
        return MultiLineString(
            [_linestring(linestring) for linestring in multilinestring.geoms]
        )

    def _linearring(linearring: LinearRing) -> LinearRing:
        return LinearRing(((x, y) for x, y in zip(*func(linearring.xy))))

    def _polygon(polygon: Polygon) -> Polygon:
        return Polygon(
            _linearring(polygon.exterior),
            holes=list(map(_linearring, polygon.interiors)),
        )

    def _multipolygon(multipolygon: MultiPolygon) -> MultiPolygon:
        return MultiPolygon([_polygon(polygon) for polygon in multipolygon.geoms])

    def _geometrycollection(
        geometrycollection: GeometryCollection,
    ) -> GeometryCollection:
        return GeometryCollection(
            [_any_geometry(subgeometry) for subgeometry in geometrycollection.geoms]
        )

    def _any_geometry(geometry: Geometry) -> Geometry:
        transform_funcs = {
            Point: _point,
            MultiPoint: _multipoint,
            LineString: _linestring,
            LinearRing: _linearring,
            MultiLineString: _multilinestring,
            Polygon: _polygon,
            MultiPolygon: _multipolygon,
            GeometryCollection: _geometrycollection,
        }
        try:
            return transform_funcs[type(geometry)](geometry)
        except KeyError:  # pragma: no cover
            raise TypeError(f"unknown geometry {geometry} of type {type(geometry)}")

    if geometry.is_empty:
        return geometry

    # make valid by buffering
    return repair(_any_geometry(geometry))
