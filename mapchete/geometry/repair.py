from shapely.errors import TopologicalError
from shapely.validation import explain_validity

from mapchete.geometry.types import Geometry, MultiPolygon, Polygon


def repair(geometry: Geometry) -> Geometry:
    repaired = (
        geometry.buffer(0)
        if isinstance(geometry, (Polygon, MultiPolygon))
        else geometry
    )
    if repaired.is_valid:
        return repaired
    else:
        raise TopologicalError(
            "geometry is invalid (%s) and cannot be repaired"
            % explain_validity(repaired)
        )
