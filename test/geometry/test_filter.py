import pytest
from pytest_lazyfixture import lazy_fixture
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)

from mapchete.geometry import (
    filter_by_geometry_type,
    get_multipart_type,
    is_type,
    multipart_to_singleparts,
)
from mapchete.geometry.types import Geometry, SinglepartGeometry


@pytest.mark.parametrize(
    "geometry",
    [
        lazy_fixture("point"),
        lazy_fixture("multipoint"),
        lazy_fixture("linestring"),
        lazy_fixture("multilinestring"),
        lazy_fixture("polygon"),
        lazy_fixture("multipolygon"),
        lazy_fixture("geometrycollection"),
    ],
)
def test_multiparts_to_singleparts(geometry):
    if isinstance(geometry, GeometryCollection):
        for subgeometry in multipart_to_singleparts(geometry):
            for subsubgeometry in multipart_to_singleparts(subgeometry):
                assert isinstance(subsubgeometry, SinglepartGeometry)
    else:
        for subgeometry in multipart_to_singleparts(geometry):
            assert isinstance(subgeometry, SinglepartGeometry)


@pytest.mark.parametrize(
    "allow_multipart",
    [True, False],
)
@pytest.mark.parametrize(
    "target_type",
    [
        Point,
        MultiPoint,
        LineString,
        MultiLineString,
        Polygon,
        MultiPolygon,
    ],
)
@pytest.mark.parametrize(
    "geometry",
    [
        lazy_fixture("point"),
        lazy_fixture("multipoint"),
        lazy_fixture("linestring"),
        lazy_fixture("multilinestring"),
        lazy_fixture("polygon"),
        lazy_fixture("multipolygon"),
        lazy_fixture("geometrycollection"),
    ],
)
def test_is_type(geometry, target_type, allow_multipart):
    if isinstance(geometry, target_type):
        assert is_type(geometry, target_type, allow_multipart=allow_multipart)

    elif allow_multipart and (isinstance(geometry, get_multipart_type(target_type))):
        assert is_type(geometry, target_type, allow_multipart=allow_multipart)

    else:
        assert not is_type(geometry, target_type, allow_multipart=allow_multipart)


@pytest.mark.parametrize(
    "allow_multipart",
    [True, False],
)
@pytest.mark.parametrize(
    "target_type",
    [
        Point,
        MultiPoint,
        LineString,
        MultiLineString,
        Polygon,
        MultiPolygon,
    ],
)
@pytest.mark.parametrize(
    "geometry",
    [
        lazy_fixture("point"),
        lazy_fixture("multipoint"),
        lazy_fixture("linestring"),
        lazy_fixture("multilinestring"),
        lazy_fixture("polygon"),
        lazy_fixture("multipolygon"),
        lazy_fixture("geometrycollection"),
    ],
)
def test_filter_by_geometry_type(geometry, target_type, allow_multipart):
    geometries = list(
        filter_by_geometry_type(
            geometry, target_type=target_type, allow_multipart=allow_multipart
        )
    )

    if isinstance(
        geometry, (target_type, get_multipart_type(target_type))
    ) or isinstance(geometry, GeometryCollection):
        assert geometries
        for geometry in geometries:
            if allow_multipart:
                assert get_multipart_type(type(geometry)) == get_multipart_type(
                    target_type
                )
            else:
                assert isinstance(geometry, target_type)

    else:
        assert not geometries
