import pytest

from mapchete.errors import GeometryTypeError
from mapchete.geometry.types import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
    get_geometry_type,
    get_multipart_type,
    get_singlepart_type,
)


@pytest.mark.parametrize(
    "geom_type,control",
    [
        (Point, MultiPoint),
        (LineString, MultiLineString),
        (Polygon, MultiPolygon),
        (MultiPoint, MultiPoint),
        (MultiLineString, MultiLineString),
        (MultiPolygon, MultiPolygon),
        (GeometryCollection, GeometryCollection),
    ],
)
def test_get_multipart_type(geom_type, control):
    assert get_multipart_type(geom_type) == control


@pytest.mark.parametrize(
    "geom_type,control",
    [
        (Point, Point),
        (LineString, LineString),
        (Polygon, Polygon),
        (MultiPoint, Point),
        (MultiLineString, LineString),
        (MultiPolygon, Polygon),
    ],
)
def test_get_singlepart_type(geom_type, control):
    assert get_singlepart_type(geom_type) == control


def test_get_singlepart_type_goemetrycollection():
    with pytest.raises(GeometryTypeError):
        get_singlepart_type(GeometryCollection)


@pytest.mark.parametrize(
    "geom_type,control",
    [
        (Point, Point),
        (LineString, LineString),
        (Polygon, Polygon),
        (MultiPoint, MultiPoint),
        (MultiLineString, MultiLineString),
        (MultiPolygon, MultiPolygon),
        (GeometryCollection, GeometryCollection),
        ("Point", Point),
        ("LineString", LineString),
        ("Polygon", Polygon),
        ("MultiPoint", MultiPoint),
        ("MultiLineString", MultiLineString),
        ("MultiPolygon", MultiPolygon),
        ("GeometryCollection", GeometryCollection),
        ("Point".lower(), Point),
        ("LineString".lower(), LineString),
        ("Polygon".lower(), Polygon),
        ("MultiPoint".lower(), MultiPoint),
        ("MultiLineString".lower(), MultiLineString),
        ("MultiPolygon".lower(), MultiPolygon),
        ("GeometryCollection".lower(), GeometryCollection),
        ("Point".upper(), Point),
        ("LineString".upper(), LineString),
        ("Polygon".upper(), Polygon),
        ("MultiPoint".upper(), MultiPoint),
        ("MultiLineString".upper(), MultiLineString),
        ("MultiPolygon".upper(), MultiPolygon),
        ("GeometryCollection".upper(), GeometryCollection),
    ],
)
def test_get_geometry_type(geom_type, control):
    assert get_geometry_type(geom_type) == control


@pytest.mark.parametrize("geom_type", ["foo", GeometryTypeError])
def test_get_geometry_type_error(geom_type):
    with pytest.raises(GeometryTypeError):
        assert get_geometry_type(geom_type)
