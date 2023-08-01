import json

import pytest
from shapely.geometry import shape

from mapchete.types import Bounds, ZoomLevels


def test_bounds_cls():
    bounds = Bounds(1, 2, 3, 4)
    assert bounds.left == 1
    assert bounds.bottom == 2
    assert bounds.right == 3
    assert bounds.top == 4


@pytest.mark.parametrize(
    "args",
    [
        [1, 2, 3, 4],
        (1, 2, 3, 4),
        dict(left=1, bottom=2, right=3, top=4),
        Bounds(1, 2, 3, 4),
    ],
)
def test_bounds_from_inp(args):
    bounds = Bounds.from_inp(args)
    assert bounds.left == 1
    assert bounds.bottom == 2
    assert bounds.right == 3
    assert bounds.top == 4


def test_bounds_subscriptable():
    bounds = Bounds(1, 2, 3, 4)
    assert bounds[0] == 1
    assert bounds[1] == 2
    assert bounds[2] == 3
    assert bounds[3] == 4
    assert bounds["left"] == 1
    assert bounds["bottom"] == 2
    assert bounds["right"] == 3
    assert bounds["top"] == 4


def test_bounds_list():
    bounds = Bounds(1, 2, 3, 4)
    assert list(bounds) == [1, 2, 3, 4]


def test_bounds_tuple():
    bounds = Bounds(1, 2, 3, 4)
    assert tuple(bounds) == (1, 2, 3, 4)


def test_bounds_dict():
    bounds = Bounds(1, 2, 3, 4)
    assert bounds.to_dict() == {"left": 1, "bottom": 2, "right": 3, "top": 4}


def test_bounds_json_serializable():
    bounds = Bounds(1, 2, 3, 4)
    assert json.dumps(bounds) == "[1, 2, 3, 4]"


def test_bounds_geo_interface():
    bounds = Bounds(1, 2, 3, 4)
    assert shape(bounds).is_valid


def test_bounds_geometry():
    bounds = Bounds(1, 2, 3, 4)
    assert bounds.geometry.is_valid


def test_bounds_width():
    assert Bounds(1, 2, 3, 4).width == 2


def test_bounds_height():
    assert Bounds(1, 2, 3, 4).height == 2


def test_bounds_errors():
    with pytest.raises(TypeError):
        Bounds(1, 2, None, 3)
    with pytest.raises(ValueError):
        Bounds(3, 2, 1, 4)
    with pytest.raises(ValueError):
        Bounds(1, 4, 3, 2)
    with pytest.raises(ValueError):
        Bounds.from_inp([1, 4, 3])
    with pytest.raises(TypeError):
        Bounds.from_inp("invalid")
    bounds = Bounds(1, 2, 3, 4)
    with pytest.raises(TypeError):
        bounds[None]
    with pytest.raises(KeyError):
        bounds["foo"]
    with pytest.raises(IndexError):
        bounds[5]


@pytest.mark.parametrize(
    "intersecting",
    [
        (-121.2451171875, 75.7177734375, -120.849609375, 76.025390625),
        (-119.3994140625, 75.498046875, -117.4658203125, 76.11328125),
        (-122.958984375, 75.8056640625, -115.400390625, 77.5634765625),
        (-125.859375, 71.0595703125, -115.3125, 74.5751953125),
        (-115.048828125, 77.6953125, -113.5986328125, 78.0908203125),
        (-114.8291015625, 76.6845703125, -113.4228515625, 76.904296875),
        (-113.3349609375, 77.34375, -109.5556640625, 78.134765625),
        (-109.423828125, 67.8955078125, -109.2041015625, 68.0712890625),
        (-113.3349609375, 78.2666015625, -109.2041015625, 78.75),
        (-108.017578125, 73.5205078125, -107.578125, 73.6083984375),
        (-117.685546875, 74.3994140625, -105.380859375, 76.8603515625),
        (-105.1171875, 68.37890625, -104.4140625, 68.5986328125),
        (-106.083984375, 77.080078125, -103.974609375, 77.783203125),
        (-104.8974609375, 75.0146484375, -103.5791015625, 75.4541015625),
        (-104.677734375, 76.3330078125, -103.0078125, 76.6845703125),
        (-102.2607421875, 68.5546875, -101.689453125, 68.818359375),
        (-102.5244140625, 77.6953125, -100.9423828125, 77.9150390625),
        (-119.1357421875, 68.466796875, -100.8544921875, 73.7841796875),
        (-101.6015625, 76.552734375, -100.4150390625, 76.7724609375),
        (-100.634765625, 70.4443359375, -100.1953125, 70.6640625),
        (-100.5908203125, 68.6865234375, -99.9755859375, 69.169921875),
        (-105.64453125, 77.783203125, -98.9208984375, 79.4091796875),
        (-100.1513671875, 79.6728515625, -98.6572265625, 80.15625),
        (-99.404296875, 73.828125, -97.6025390625, 74.1357421875),
        (-104.4580078125, 74.970703125, -97.3388671875, 76.6845703125),
        (-97.7783203125, 74.443359375, -97.2509765625, 74.619140625),
        (-97.119140625, 72.9052734375, -96.5478515625, 73.212890625),
        (-96.9873046875, 75.3662109375, -96.416015625, 75.5859375),
        (-102.744140625, 71.279296875, -96.2841796875, 73.9599609375),
        (-96.591796875, 69.345703125, -96.064453125, 69.5654296875),
        (-96.3720703125, 75.4541015625, -95.9326171875, 75.673828125),
        (-95.9765625, 69.3017578125, -95.361328125, 69.6533203125),
        (-99.5361328125, 68.466796875, -95.2294921875, 69.9169921875),
        (-95.7568359375, 74.4873046875, -95.2294921875, 74.6630859375),
        (-98.3935546875, 77.783203125, -94.833984375, 78.837890625),
        (-94.658203125, 78.1787109375, -94.306640625, 78.2666015625),
        (-94.8779296875, 75.76171875, -94.2626953125, 75.9814453125),
        (-96.6357421875, 74.619140625, -93.3837890625, 75.6298828125),
        (-96.2841796875, 77.431640625, -93.1201171875, 77.8271484375),
        (-91.23046875, 77.1240234375, -90.703125, 77.255859375),
        (-90.0, 68.5986328125, -89.9560546875, 68.642578125),
        (-90.0439453125, 68.6865234375, -89.9560546875, 68.818359375),
        (-90.087890625, 71.8505859375, -89.9560546875, 72.0703125),
        (-96.8994140625, 74.53125, -89.9560546875, 77.255859375),
        (-90.5712890625, 76.46484375, -89.9560546875, 76.8603515625),
        (-91.1865234375, 77.2119140625, -89.9560546875, 77.6513671875),
        (-96.7236328125, 78.134765625, -89.9560546875, 81.38671875),
        (-168.134765625, 13.6669921875, -89.9560546875, 74.1796875),
    ],
)
def test_bounds_intersect_custom(intersecting):
    bounds1 = Bounds(-135.0, 60, -90, 80)
    bounds2 = Bounds(*intersecting)
    # internal intersects()
    assert bounds1.intersects(bounds2)
    # shapely geometry intersects()
    assert shape(bounds1).intersects(shape(bounds2))


def test_bounds_equal():
    assert Bounds(1, 2, 3, 4) == Bounds(1, 2, 3, 4)
    assert Bounds(1, 2, 3, 4) == Bounds(1.0, 2.0, 3.0, 4.0)


def test_bounds_not_equal():
    assert Bounds(1, 2, 3, 4) != Bounds(0, 2, 3, 4)
    assert Bounds(1, 2, 3, 4) != Bounds(0.0, 2.0, 3.0, 4.0)


def test_bounds_str_repr():
    bounds = Bounds(1, 2, 3, 4)
    assert str(bounds) == repr(bounds)


def test_zoom_levels_from_int():
    zoom_levels = ZoomLevels.from_int(4)
    assert len(zoom_levels) == 1
    assert list(zoom_levels) == [4]
    assert zoom_levels.min == 4
    assert zoom_levels.max == 4


def test_zoom_levels_from_list():
    zoom_levels = ZoomLevels.from_list([2, 4])
    assert len(zoom_levels) == 3
    assert list(zoom_levels) == [2, 3, 4]
    assert zoom_levels.min == 2
    assert zoom_levels.max == 4


def test_zoom_levels_from_inverted_list():
    zoom_levels = ZoomLevels.from_list([4, 2])
    assert len(zoom_levels) == 3
    assert list(zoom_levels) == [2, 3, 4]
    assert zoom_levels.min == 2
    assert zoom_levels.max == 4


def test_zoom_levels_from_dict():
    zoom_levels = ZoomLevels.from_dict(dict(min=2, max=4))
    assert len(zoom_levels) == 3
    assert list(zoom_levels) == [2, 3, 4]
    assert zoom_levels.min == 2
    assert zoom_levels.max == 4


def test_zoom_levels_from_inp_int():
    zoom_levels = ZoomLevels.from_inp(4)
    assert len(zoom_levels) == 1
    assert list(zoom_levels) == [4]
    assert zoom_levels.min == 4
    assert zoom_levels.max == 4


@pytest.mark.parametrize(
    "inp",
    [
        [2, 4],
        [4, 2],
        [2, 3, 4],
        dict(min=2, max=4),
        ZoomLevels(min=2, max=4),
        ZoomLevels(min=2, max=4, descending=True),
    ],
)
def test_zoom_levels_from_inp(inp):
    zoom_levels = ZoomLevels.from_inp(inp)
    assert len(zoom_levels) == 3
    assert list(zoom_levels) == [2, 3, 4]
    assert zoom_levels.min == 2
    assert zoom_levels.max == 4


def test_zoom_levels_from_kwargs():
    zoom_levels = ZoomLevels(min=2, max=4)
    assert len(zoom_levels) == 3
    assert list(zoom_levels) == [2, 3, 4]
    assert zoom_levels.min == 2
    assert zoom_levels.max == 4


def test_zoom_levels_subscriptable():
    zoom_levels = ZoomLevels(2, 4)
    assert zoom_levels[0] == 2
    assert zoom_levels[1] == 3
    assert zoom_levels[2] == 4
    assert zoom_levels["min"] == 2
    assert zoom_levels["max"] == 4


def test_zoom_levels_intersects():
    assert ZoomLevels(2, 4).intersects(ZoomLevels(2, 4))
    assert ZoomLevels(2, 4).intersects(ZoomLevels(4, 4))
    assert ZoomLevels(2, 4).intersects(ZoomLevels(3, 5))
    assert ZoomLevels(2, 4).intersects(ZoomLevels(4, 5))
    assert ZoomLevels(2, 4).intersects(ZoomLevels(1, 3))
    assert ZoomLevels(2, 4).intersects(ZoomLevels(1, 4))
    assert ZoomLevels(2, 4).intersects(ZoomLevels(1, 5))

    assert not ZoomLevels(2, 4).intersects(ZoomLevels(0, 1))
    assert not ZoomLevels(2, 4).intersects(ZoomLevels(5, 6))


def test_zoom_levels_errors():
    with pytest.raises(TypeError):
        ZoomLevels.from_inp(min="invalid")
    with pytest.raises(TypeError):
        ZoomLevels.from_inp(max="invalid")
    with pytest.raises(TypeError):
        ZoomLevels.from_inp(min=[1], max=5)
    with pytest.raises(TypeError):
        ZoomLevels.from_inp(min=dict(max=1), max=5)
    with pytest.raises(KeyError):
        ZoomLevels.from_inp(min=dict(max=1))
    with pytest.raises(TypeError):
        ZoomLevels.from_inp(min=["invalid"])
    with pytest.raises(ValueError):
        ZoomLevels.from_inp(min=[])
    with pytest.raises(ValueError):
        ZoomLevels.from_inp(min=[1, 3, 4])
    with pytest.raises(ValueError):
        ZoomLevels.from_inp(-6)
    with pytest.raises(ValueError):
        ZoomLevels(min=5, max=4)


def test_zoom_levels_json_serializable():
    zoom_levels = ZoomLevels(2, 4)
    assert json.dumps(zoom_levels) == "[2, 3, 4]"


def test_zoom_levels_contains():
    zoom_levels = ZoomLevels(2, 4)
    assert 3 in zoom_levels


def test_zoom_levels_descending():
    assert list(ZoomLevels(2, 4, descending=True)) == [4, 3, 2]
    assert list(ZoomLevels(2, 4).descending()) == [4, 3, 2]


def test_zoom_levels_minmax():
    assert min(ZoomLevels(2, 4)) == 2
    assert max(ZoomLevels(2, 4)) == 4


def test_zoom_levels_equal():
    assert ZoomLevels(2, 4) == ZoomLevels(2, 4)
    assert ZoomLevels(2, 4) == ZoomLevels(2, 4, descending=True)
    assert ZoomLevels(2, 4) != ZoomLevels(1, 4)


def test_zoom_levels_to_dict():
    assert ZoomLevels(2, 4).to_dict() == {"min": 2, "max": 4}


def test_zoom_levels_str_repr():
    zoom_levels = ZoomLevels(1, 5)
    assert str(zoom_levels) == repr(zoom_levels)
