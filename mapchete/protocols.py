from typing import Protocol, Tuple, runtime_checkable

from affine import Affine
from rasterio.crs import CRS

from mapchete.types import Bounds


class ObserverProtocol(Protocol):
    """Protocol used for custom observer classes hooked up into commands."""

    def update(self, *args, **kwargs) -> None:  # pragma: no cover
        ...


@runtime_checkable
class GridProtocol(Protocol):
    transform: Affine
    width: int
    height: int
    shape: Tuple[int, int]
    bounds: Bounds
    crs: CRS
