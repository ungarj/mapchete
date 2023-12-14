from __future__ import annotations

import os
from typing import Iterable, List, Optional, Tuple, Union

from affine import Affine
from fiona.crs import CRS as FionaCRS
from pydantic import BaseModel
from rasterio.crs import CRS as RasterioCRS
from rasterio.transform import array_bounds, from_bounds
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from tilematrix import Shape

from mapchete.tile import BufferedTile

MPathLike = Union[str, os.PathLike]
BoundsLike = Union[List[float], Tuple[float], dict, BaseGeometry]
ShapeLike = Union[Shape, List[int], Tuple[int, int]]
ZoomLevelsLike = Union[List[int], int, dict]
TileLike = Union[BufferedTile, Tuple[int, int, int]]
CRSLike = Union[FionaCRS, RasterioCRS]
NodataVal = Optional[float]
NodataVals = Union[List[NodataVal], NodataVal]


class Bounds(list):
    """
    Class to handle geographic bounds.
    """

    left: float = None
    bottom: float = None
    right: float = None
    top: float = None
    height: float = None
    width: float = None

    def __init__(
        self,
        left: Union[Iterable[float], float],
        bottom: Optional[float],
        right: Optional[float],
        top: Optional[float],
        strict: bool = True,
    ):
        self._set_attributes(left, bottom, right, top)
        for value in self:
            if not isinstance(value, (int, float)):
                raise TypeError(
                    f"all bounds values must be integers or floats: {list(self)}"
                )
        if strict:
            if self.left >= self.right:
                raise ValueError("right must be larger than left")
            elif self.bottom >= self.top:
                raise ValueError("top must be larger than bottom")
        self.height = self.top - self.bottom
        self.width = self.right - self.left

    def __iter__(self):
        yield self.left
        yield self.bottom
        yield self.right
        yield self.top

    def __len__(self):
        return 4

    def __str__(self):
        return f"<Bounds(left={self.left}, bottom={self.bottom}, right={self.right}, top={self.top})>"

    def __repr__(self):
        return str(self)

    def __getitem__(self, item):
        if isinstance(item, int):
            return list(self)[item]
        elif isinstance(item, str):
            try:
                return self.__getattribute__(item)
            except AttributeError as exc:
                raise KeyError(exc)
        else:
            raise TypeError(f"desired item '{item}' has wrong type {type(item)}")

    def __eq__(self, other):
        other = other if isinstance(other, Bounds) else Bounds.from_inp(other)
        return (
            float(self.left) == float(other.left)
            and float(self.bottom) == float(other.bottom)
            and float(self.right) == float(other.right)
            and float(self.top) == float(other.top)
        )

    def __ne__(self, other):
        return not self == other

    @property
    def __geo_interface__(self):
        return {
            "type": "Polygon",
            "bbox": tuple(self),
            "coordinates": [
                [
                    [self.left, self.bottom],
                    [self.right, self.bottom],
                    [self.right, self.top],
                    [self.left, self.top],
                    [self.left, self.bottom],
                ]
            ],
        }

    def _set_attributes(
        self,
        left: Union[Iterable[float], float],
        bottom: Optional[float],
        right: Optional[float],
        top: Optional[float],
    ) -> None:
        """This method is important when Bounds instances are passed on to the ProcessConfig schema."""
        if hasattr(left, "__iter__"):  # pragma: no cover
            self.left, self.bottom, self.right, self.top = [i for i in left]
        else:
            self.left, self.bottom, self.right, self.top = left, bottom, right, top

    @property
    def geometry(self) -> BaseGeometry:
        return shape(self)

    @classmethod
    def from_inp(cls, inp: BoundsLike, strict: bool = True) -> Bounds:
        if isinstance(inp, (list, tuple)):
            if len(inp) != 4:
                raise ValueError("Bounds must be initialized with exactly four values.")
            return Bounds(*inp, strict=strict)
        elif isinstance(inp, dict):
            return Bounds.from_dict(inp, strict=strict)
        elif isinstance(inp, BaseGeometry):
            return Bounds(*inp.bounds, strict=strict)
        else:
            raise TypeError(f"cannot create Bounds using {inp}")

    @staticmethod
    def from_dict(inp: dict, strict: bool = True) -> Bounds:
        return Bounds(**inp, strict=strict)

    def to_dict(self) -> dict:
        """Return dictionary representation."""
        return {
            "left": self.left,
            "bottom": self.bottom,
            "right": self.right,
            "top": self.top,
        }

    def intersects(self, other: BoundsLike) -> bool:
        """Indicate whether bounds intersect spatially."""
        other = other if isinstance(other, Bounds) else Bounds.from_inp(other)
        horizontal = (
            # partial overlap
            self.left <= other.left <= self.right
            or self.left <= other.right <= self.right
            # self within other
            or other.left <= self.left < self.right <= other.right
            # other within self
            or self.left <= other.left < other.right <= self.right
        )
        vertical = (
            # partial overlap
            self.bottom <= other.bottom <= self.top
            or self.bottom <= other.top <= self.top
            # self within other
            or other.bottom <= self.bottom < self.top <= other.top
            # other within self
            or self.bottom <= other.bottom < other.top <= self.top
        )
        return horizontal and vertical


class ZoomLevels(list):
    min: int = None
    max: int = None

    def __init__(
        self,
        min: Union[List[int], int],
        max: Optional[int] = None,
        descending: bool = False,
    ):
        self._set_attributes(min, max)
        # assert that min and max are positive integers
        for key, value in [("min", self.min), ("max", self.max)]:
            if not isinstance(value, int):
                raise TypeError(f"{key} is not an integer: {value}")
            elif value < 0:
                raise ValueError(f"{key} is not greater or equal than 0: {value}")
        # assert that min is not greater than max
        if self.min > self.max:
            raise ValueError(f"min ({min}) cannot be greater than max ({max})")
        self.is_descending = descending

    def __iter__(self):
        if self.is_descending:
            yield from range(self.max, self.min - 1, -1)
        else:
            yield from range(self.min, self.max + 1)

    def __len__(self):
        return self.max + 1 - self.min

    def __str__(self):
        return f"<ZoomLevels(min={self.min}, max={self.max})>"

    def __repr__(self):
        return str(self)

    def __getitem__(self, item):
        if isinstance(item, int):
            return list(self)[item]
        elif isinstance(item, str):
            return self.__getattribute__(item)

    def __eq__(self, other):
        other = other if isinstance(other, ZoomLevels) else ZoomLevels.from_inp(other)
        return self.min == other.min and self.max and other.max

    def __ne__(self, other):
        return not self == other

    def __contains__(self, value):
        return value in list(self)

    def _set_attributes(
        self, minlevel: Union[List[int], int], maxlevel: Optional[int] = None
    ) -> None:
        """This method is important when ZoomLevel instances are passed on to the ProcessConfig schema."""
        if hasattr(minlevel, "__iter__"):  # pragma: no cover
            zoom_list = [i for i in minlevel]
            self.min = min(zoom_list)
            self.max = max(zoom_list)
        else:
            self.min, self.max = minlevel, maxlevel

    @classmethod
    def from_inp(
        cls, min: ZoomLevelsLike, max: Optional[int] = None, descending: bool = False
    ) -> ZoomLevels:
        """Constructs ZoomLevels from various input forms"""
        if isinstance(min, int) and max is None:
            return cls.from_int(min, descending=descending)
        elif isinstance(min, ZoomLevels) and max is None:
            if min.is_descending == descending:
                return min
            else:
                return ZoomLevels(min=min.min, max=min.max, descending=descending)
        elif isinstance(min, list) and max is None:
            return cls.from_list(min, descending=descending)
        elif isinstance(min, dict) and max is None:
            return cls.from_dict(min, descending=descending)
        else:
            raise TypeError(f"cannot create ZoomLevels with min={min}, max={max}")

    @staticmethod
    def from_int(inp: int, **kwargs) -> ZoomLevels:
        return ZoomLevels(min=inp, max=inp, **kwargs)

    @staticmethod
    def from_list(inp: List[int], **kwargs) -> ZoomLevels:
        if len(inp) == 0:
            raise ValueError("zoom level list is empty")
        elif len(inp) == 1:
            return ZoomLevels(min=inp[0], max=inp[0], **kwargs)
        elif len(inp) == 2:
            return ZoomLevels(min=min(inp), max=max(inp), **kwargs)
        else:
            if set(inp) != set(range(min(inp), max(inp) + 1)):
                raise ValueError(
                    f"zoom level list must be a full sequence without missing zoom levels: {inp}"
                )
            return ZoomLevels(min=min(inp), max=max(inp), **kwargs)

    @staticmethod
    def from_dict(inp: dict, **kwargs) -> ZoomLevels:
        try:
            return ZoomLevels(min=inp["min"], max=inp["max"], **kwargs)
        except KeyError:
            raise KeyError(f"dict does not contain 'min' and 'max' keys: {inp}")

    def to_dict(self) -> dict:
        return {
            "min": self.min,
            "max": self.max,
        }

    def intersection(self, other: ZoomLevelsLike) -> ZoomLevels:
        other = other if isinstance(other, ZoomLevels) else ZoomLevels(other)
        intersection = set(self).intersection(set(other))
        if len(intersection) == 0:
            raise ValueError("ZoomLevels do not intersect")
        return ZoomLevels(min(intersection), max(intersection))

    def difference(self, other: ZoomLevelsLike) -> ZoomLevels:
        other = other if isinstance(other, ZoomLevels) else ZoomLevels(other)
        difference = set(self).difference(set(other))
        if len(difference) == 0:  # pragma: no cover
            raise ValueError("ZoomLevels do not differ")
        return ZoomLevels(min(difference), max(difference))

    def intersects(self, other: ZoomLevelsLike) -> bool:
        other = other if isinstance(other, ZoomLevels) else ZoomLevels(other)
        try:
            return len(self.intersection(other)) > 0
        except ValueError:
            return False

    def descending(self) -> ZoomLevels:
        return ZoomLevels(min=self.min, max=self.max, descending=True)


class Progress(BaseModel):
    current: int = 0
    total: Optional[int] = None


class Grid:
    transform: Affine
    height: int
    width: int
    crs: CRSLike
    bounds: Bounds
    shape: Shape

    def __init__(self, transform: Affine, height: int, width: int, crs: CRSLike):
        self.transform = transform
        self.height = height
        self.width = width
        self.crs = crs
        self.bounds = Bounds(*array_bounds(self.height, self.width, self.transform))
        self.shape = Shape(self.height, self.width)

    @staticmethod
    def from_obj(obj):  # pragma: no cover
        if hasattr(obj, "transform"):
            transform = obj.transform
        else:
            transform = obj.affine
        return Grid(transform, obj.height, obj.width, obj.crs)

    @staticmethod
    def from_bounds(bounds: BoundsLike, shape: ShapeLike, crs: CRSLike) -> Grid:
        shape = Shape(*shape)
        bounds = Bounds.from_inp(bounds)
        transform = from_bounds(
            bounds.left,
            bounds.bottom,
            bounds.right,
            bounds.top,
            shape.width,
            shape.height,
        )
        return Grid(transform, shape.height, shape.width, crs)

    def to_dict(self):  # pragma: no cover
        return dict(
            transform=self.transform,
            height=self.height,
            width=self.width,
            crs=self.crs,
            bounds=self.bounds,
            shape=self.shape,
        )
