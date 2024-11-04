from shapely.errors import TopologicalError
from shapely.validation import explain_validity

from mapchete.geometry.types import Geometry, MultiPolygon, Polygon


def repair(geometry: Geometry, normalize: bool = True) -> Geometry:
    if isinstance(geometry, (Polygon, MultiPolygon)):
        out = geometry.buffer(0)
    else:
        out = geometry

    if normalize:
        out = out.normalize()

    if out.is_valid:
        return out
    else:
        raise TopologicalError(
            f"geometry is invalid ({explain_validity(out)}) and cannot be repaired"
        )
