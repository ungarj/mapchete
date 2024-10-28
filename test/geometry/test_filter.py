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

from mapchete.errors import GeometryTypeError
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
    "singlepart_equivalent_matches",
    [True, False],
)
def test_is_type_singleparts(geometry, target_type, singlepart_equivalent_matches):
    geometry_type = get_geometry_type(geometry.geom_type)
    if singlepart_equivalent_matches:
        try:
            control = (
                geometry_type == target_type
                or get_singlepart_type(geometry_type) == target_type
            )
        except GeometryTypeError:
            control = geometry_type == target_type
    else:
        control = geometry_type == target_type
    assert (
        is_type(
            geometry,
            target_type,
            singlepart_equivalent_matches=singlepart_equivalent_matches,
            multipart_equivalent_matches=False,
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
    "multipart_equivalent_matches",
    [True, False],
)
def test_is_type_multiparts(geometry, target_type, multipart_equivalent_matches):
    geometry_type = get_geometry_type(geometry.geom_type)
    if multipart_equivalent_matches:
        try:
            control = (
                geometry_type == target_type
                or get_multipart_type(geometry_type) == target_type
            )
        except GeometryTypeError:
            control = geometry_type == target_type
    else:
        control = geometry_type == target_type
    assert (
        is_type(
            geometry,
            target_type,
            multipart_equivalent_matches=multipart_equivalent_matches,
            singlepart_equivalent_matches=False,
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
def test_is_type_tuple(geometry, target_type):
    assert is_type(geometry, (target_type, geometry.geom_type))


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
    "singlepart_equivalent_matches",
    [True, False],
)
@pytest.mark.parametrize(
    "multipart_equivalent_matches",
    [True, False],
)
def test_filter_by_geometry_type(
    geometry, target_type, singlepart_equivalent_matches, multipart_equivalent_matches
):
    filtered_geometries = list(
        filter_by_geometry_type(
            geometry,
            target_type=target_type,
            singlepart_equivalent_matches=singlepart_equivalent_matches,
            multipart_equivalent_matches=multipart_equivalent_matches,
        )
    )
    if is_type(
        geometry,
        target_type=target_type,
        singlepart_equivalent_matches=True,
        multipart_equivalent_matches=multipart_equivalent_matches,
    ) or is_type(geometry, GeometryCollection):
        assert filtered_geometries
    else:
        assert not filtered_geometries
    for filtered_geometry in filtered_geometries:
        assert is_type(filtered_geometry, target_type)
