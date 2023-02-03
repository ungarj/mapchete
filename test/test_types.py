import json
import pytest

from mapchete.types import Bounds


@pytest.mark.parametrize(
    "args",
    [
        [1, 2, 3, 4],
        [[1, 2, 3, 4]],
        [(1, 2, 3, 4)],
    ],
)
def test_bounds_cls(args):
    bounds = Bounds(*args)
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


# def test_bounds_dict():
#     bounds = Bounds(1, 2, 3, 4)
#     assert dict(bounds) == {'left': 1, 'bottom': 2, 'right': 3, 'top': 4}


def test_bounds_json_serializable():
    bounds = Bounds(1, 2, 3, 4)
    assert json.dumps(bounds) == "[1, 2, 3, 4]"
