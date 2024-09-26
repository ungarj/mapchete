import json
import pytest

from mapchete.zoom_levels import ZoomLevels


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
        ZoomLevels.from_inp(min="invalid")  # type: ignore
    with pytest.raises(TypeError):
        ZoomLevels.from_inp(max="invalid")  # type: ignore
    with pytest.raises(TypeError):
        ZoomLevels.from_inp(min=[1], max=5)
    with pytest.raises(TypeError):
        ZoomLevels.from_inp(min=dict(max=1), max=5)
    with pytest.raises(KeyError):
        ZoomLevels.from_inp(min=dict(max=1))
    with pytest.raises(TypeError):
        ZoomLevels.from_inp(min=["invalid"])  # type: ignore
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
