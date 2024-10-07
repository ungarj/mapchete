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
from mapchete.geometry.types import (
    SinglepartGeometry,
    get_geometry_type,
    get_singlepart_type,
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
def test_multiparts_to_singleparts(geometry):
    if isinstance(geometry, GeometryCollection):
        for subgeometry in multipart_to_singleparts(geometry):
            for subsubgeometry in multipart_to_singleparts(subgeometry):
                assert isinstance(subsubgeometry, SinglepartGeometry)
    else:
        for subgeometry in multipart_to_singleparts(geometry):
            assert isinstance(subgeometry, SinglepartGeometry)


@pytest.mark.parametrize(
    "geometry",
    [
        lazy_fixture("point"),
        lazy_fixture("multipoint"),
        lazy_fixture("linestring"),
        # lazy_fixture("multilinestring"),
        # lazy_fixture("polygon"),
        # lazy_fixture("multipolygon"),
        # lazy_fixture("geometrycollection"),
    ],
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
    "singlepart_matches_multipart",
    [True, False],
)
def test_is_type_singleparts(geometry, target_type, singlepart_matches_multipart):
    geometry_type = get_geometry_type(geometry.geom_type)
    if singlepart_matches_multipart:
        control = geometry_type == get_singlepart_type(target_type)
    else:
        control = geometry_type == target_type
    assert (
        is_type(
            geometry,
            target_type,
            singlepart_matches_multipart=singlepart_matches_multipart,
        )
        == control
    )


@pytest.mark.parametrize(
    "geometry",
    [
        lazy_fixture("point"),
        lazy_fixture("multipoint"),
        lazy_fixture("linestring"),
        # lazy_fixture("multilinestring"),
        # lazy_fixture("polygon"),
        # lazy_fixture("multipolygon"),
        # lazy_fixture("geometrycollection"),
    ],
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
    "multipart_matches_singlepart",
    [True, False],
)
def test_is_type_multiparts(geometry, target_type, multipart_matches_singlepart):
    geometry_type = get_geometry_type(geometry.geom_type)
    if multipart_matches_singlepart:
        control = geometry_type == get_multipart_type(target_type)
    else:
        control = geometry_type == target_type
    assert (
        is_type(
            geometry,
            target_type,
            singlepart_matches_multipart=multipart_matches_singlepart,
        )
        == control
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
    "singlepart_matches_multipart",
    [True, False],
)
@pytest.mark.parametrize(
    "multipart_matches_singlepart",
    [True, False],
)
def test_filter_by_geometry_type(
    geometry, target_type, singlepart_matches_multipart, multipart_matches_singlepart
):
    geometries = list(
        filter_by_geometry_type(
            geometry,
            target_type=target_type,
            singlepart_matches_multipart=singlepart_matches_multipart,
            multipart_matches_singlepart=multipart_matches_singlepart,
        )
    )
    geometry_type = get_geometry_type(geometry.geom_type)
    if singlepart_matches_multipart and (
        geometry_type == target_type or get_multipart_type(geometry_type) == target_type
    ):
        assert geometries

    elif (
        multipart_matches_singlepart
        and geometry_type != GeometryCollection
        and (
            geometry_type == target_type
            or get_singlepart_type(geometry_type) == target_type
        )
    ):
        assert geometries

    else:
        assert not geometries


@pytest.mark.parametrize(
    "geometry",
    [
        lazy_fixture("geometrycollection"),
    ],
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
        GeometryCollection,
    ],
)
@pytest.mark.parametrize(
    "singlepart_matches_multipart",
    [True, False],
)
@pytest.mark.parametrize(
    "multipart_matches_singlepart",
    [True, False],
)
def test_filter_by_geometry_type_geometrycollection(
    geometry, target_type, singlepart_matches_multipart, multipart_matches_singlepart
):
    geometries = list(
        filter_by_geometry_type(
            geometry,
            target_type=target_type,
            singlepart_matches_multipart=singlepart_matches_multipart,
            multipart_matches_singlepart=multipart_matches_singlepart,
        )
    )
    geometry_type = get_geometry_type(geometry.geom_type)
    if singlepart_matches_multipart and (
        geometry_type == target_type or get_multipart_type(geometry_type) == target_type
    ):
        assert geometries

    else:
        assert not geometries
