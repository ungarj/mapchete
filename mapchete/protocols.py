from __future__ import annotations

from typing import (
    Any,
    Callable,
    List,
    NoReturn,
    Optional,
    Protocol,
    Tuple,
    Union,
    runtime_checkable,
)

import numpy.ma as ma
from affine import Affine
from rasterio.crs import CRS
from shapely.geometry.base import BaseGeometry

from mapchete.tile import BufferedTilePyramid
from mapchete.types import Bounds, BoundsLike, CRSLike, ResamplingLike, TileLike


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


class InputTileProtocol(Protocol):
    preprocessing_tasks_results: dict = {}
    input_key: str

    def read(self, **kwargs) -> Any:
        """Read from input."""
        ...

    def is_empty(self) -> bool:
        """Checks if input is empty here."""
        ...

    def set_preprocessing_task_result(self, task_key: str, result: Any) -> NoReturn:
        ...

    def __enter__(self) -> InputTileProtocol:
        """Required for 'with' statement."""
        return self

    def __exit__(self, *args):
        """Clean up."""


class RasterInput(InputTileProtocol):
    def read(
        self,
        indexes: Optional[Union[List[int], int]] = None,
        resampling: Optional[ResamplingLike] = None,
        **kwargs,
    ) -> ma.MaskedArray:
        """Read resampled array from input."""
        ...


class VectorInput(InputTileProtocol):
    def read(
        self, validity_check: bool = True, clip_to_crs_bounds: bool = False, **kwargs
    ) -> List[dict]:
        """Read reprojected and clipped vector features from input."""
        ...


RasterInputGroup = List[RasterInput]
VectorInputGroup = List[VectorInput]


class InputDataProtocol(Protocol):
    input_key: str
    pyramid: BufferedTilePyramid
    pixelbuffer: int = 0
    crs: CRSLike
    preprocessing_tasks: dict = {}
    preprocessing_tasks_results: dict = {}

    def open(self, tile: TileLike, **kwargs) -> InputTileProtocol:
        ...

    def bbox(self, out_crs: Optional[CRSLike] = None) -> BaseGeometry:
        ...

    def exists(self) -> bool:
        ...

    def cleanup(self) -> NoReturn:
        ...

    def add_preprocessing_task(
        self,
        func: Callable,
        fargs: Optional[tuple] = None,
        fkwargs: Optional[dict] = None,
        key: Optional[str] = None,
        geometry: Optional[BaseGeometry] = None,
        bounds: Optional[BoundsLike] = None,
    ) -> NoReturn:
        ...

    def get_preprocessing_task_result(self, task_key: str) -> Any:
        ...

    def set_preprocessing_task_result(self, task_key: str, result: Any) -> NoReturn:
        ...

    def preprocessing_task_finished(self, task_key: str) -> bool:
        ...
