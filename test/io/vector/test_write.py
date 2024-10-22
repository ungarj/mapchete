from fiona.errors import DriverError
import pytest

from mapchete.io.vector import fiona_open, write_vector_window
from mapchete.tile import BufferedTilePyramid


def test_write_vector_window_errors(landpoly):
    with fiona_open(str(landpoly)) as src:
        feature = next(iter(src))
    with pytest.raises((DriverError, ValueError, TypeError)):
        write_vector_window(
            in_data=["invalid", feature],
            out_tile=BufferedTilePyramid("geodetic").tile(0, 0, 0),
            out_path="/invalid_path",
            out_schema=dict(geometry="Polygon", properties=dict()),
        )
