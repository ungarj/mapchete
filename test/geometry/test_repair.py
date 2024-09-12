import pytest
from shapely.errors import TopologicalError
from shapely.geometry import LineString

from mapchete.geometry import repair


def test_repair_geometry():
    # invalid LineString
    line = LineString([(0, 0), (0, 0), (0, 0)])
    with pytest.raises(TopologicalError):
        repair(line)
