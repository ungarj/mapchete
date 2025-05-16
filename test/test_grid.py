import pytest

from mapchete.bounds import Bounds
from mapchete.grid import Grid


@pytest.mark.parametrize(
    "bounds",
    [
        (0, 0, 0.4, 0.6),
        (0, 0, 0.5, 0.5),
        (0, 0, 0.6, 0.4),
        # (0, 0, 1.1, 1.1),
        (0.4, 0.6, 1, 1),
        (0.5, 0.5, 1, 1),
        (0.6, 0.4, 1, 1),
        # (-0.1, -0.1, 1, 1),
    ],
)
@pytest.mark.parametrize("grid_size", [1, 2, 3])
def test_extract(bounds, grid_size):
    # 3x3 grid
    grid1 = Grid.from_bounds(
        bounds=(0, 0, grid_size, grid_size),
        shape=(grid_size, grid_size),
        crs="EPSG:4326",
    )

    # extract from bounds smaller than the pixel size
    grid2 = grid1.extract(bounds)

    # make sure grid2 is snapped to grid1
    assert grid2.shape == (1, 1)
    assert grid2.bounds == Bounds.from_inp((0, 0, 1, 1))
