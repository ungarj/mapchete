import pytest
from fiona.crs import CRS  # type: ignore
from pytest_lazyfixture import lazy_fixture

from mapchete.geometry.latlon import (
    latlon_to_utm_crs,
    longitudinal_shift,
    transform_to_latlon,
)
from mapchete.types import Bounds


@pytest.mark.parametrize(
    "polygon",
    [
        lazy_fixture("antimeridian_polygon1"),
        lazy_fixture("antimeridian_polygon2"),
        lazy_fixture("antimeridian_polygon3"),
    ],
)
def test_longitudinal_shift(polygon):
    shifted = longitudinal_shift(polygon)
    assert Bounds.from_inp(shifted).right > 180


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
def test_latlon_to_utm_crs(geometry):
    assert (
        latlon_to_utm_crs(geometry.centroid.x, geometry.centroid.y).to_epsg() == 32631
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
def test_transform_to_latlon(geometry):
    assert transform_to_latlon(geometry, CRS.from_epsg(3857)).is_valid
