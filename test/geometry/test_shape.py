import pytest
from shapely.geometry import box, mapping

from mapchete.geometry import to_shape


class GeoObject1:
    __geo_interface__ = mapping(box(0, 1, 2, 3))


class GeoObject2:
    __geo_interface__ = dict(geometry=mapping(box(0, 1, 2, 3)))


@pytest.mark.parametrize(
    "obj",
    [box(0, 1, 2, 3), mapping(box(0, 1, 2, 3)), GeoObject1(), GeoObject2()],
)
def test_to_shape(obj):
    assert to_shape(obj).is_valid
