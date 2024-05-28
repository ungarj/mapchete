import pytest
from shapely.geometry import LineString, box

from mapchete.geometry.segmentize import segmentize_geometry


@pytest.mark.parametrize(
    "geometry",
    [
        box(-18, -9, 18, 9),
        box(-18, -9, 18, 9).exterior,
        LineString(box(-18, -9, 18, 9).exterior),
    ],
)
def test_segmentize(geometry):
    """Segmentize function."""
    out = segmentize_geometry(geometry, 1)
    assert out.is_valid


def test_segmentize_typeerror():
    # wrong type
    with pytest.raises(TypeError):
        segmentize_geometry(box(-18, -9, 18, 9).centroid, 1)
