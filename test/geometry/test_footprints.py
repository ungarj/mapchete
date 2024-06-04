import pytest
from pytest_lazyfixture import lazy_fixture
from shapely.geometry import Polygon

from mapchete.geometry.footprints import (
    buffer_antimeridian_safe,
    repair_antimeridian_geometry,
)
from mapchete.geometry.types import MultipartGeometry
from mapchete.types import Bounds


@pytest.mark.parametrize(
    "polygon",
    [
        lazy_fixture("antimeridian_polygon1"),
        lazy_fixture("antimeridian_polygon2"),
        lazy_fixture("antimeridian_polygon3"),
    ],
)
def test_repair_antimeridian_geometry(polygon):
    fixed = repair_antimeridian_geometry(polygon)
    assert isinstance(fixed, MultipartGeometry)


@pytest.mark.parametrize("buffer", [0, -500, 500])
@pytest.mark.parametrize(
    "polygon",
    [
        lazy_fixture("antimeridian_polygon1"),
        lazy_fixture("antimeridian_polygon2"),
        lazy_fixture("antimeridian_polygon3"),
        Polygon(),
    ],
)
def test_buffer_antimeridian_safe(polygon, buffer):
    fixed_footprint = repair_antimeridian_geometry(polygon)
    buffered = buffer_antimeridian_safe(fixed_footprint, buffer_m=buffer)

    if polygon.is_empty:
        assert buffered.is_empty

    else:
        if buffer < 0:
            # buffered should be smaller than original
            assert buffered.area < fixed_footprint.area
        elif buffer > 0:
            # buffered should be smaller than original
            assert buffered.area > fixed_footprint.area
        else:
            assert buffered.area == fixed_footprint.area

        # however, it should still touch the antimeridian
        bounds = Bounds.from_inp(buffered)
        assert bounds.left == -180
        assert bounds.right == 180
