from __future__ import annotations

from typing import Tuple

from affine import Affine
from rasterio.transform import array_bounds, from_bounds, rowcol
from shapely.geometry import mapping, shape
from tilematrix import Shape

from mapchete.bounds import Bounds
from mapchete.types import CRSLike, BoundsLike, ShapeLike


class Grid:
    transform: Affine
    height: int
    width: int
    crs: CRSLike
    bounds: Bounds
    shape: Tuple[int, int]

    def __init__(self, transform: Affine, height: int, width: int, crs: CRSLike):
        self.transform = transform
        self.height = height
        self.width = width
        self.crs = crs
        self.bounds = Bounds(*array_bounds(self.height, self.width, self.transform))
        self.shape = Shape(self.height, self.width)
        self.__geo_interface__ = mapping(shape(self.bounds))

    def extract(self, bounds: BoundsLike) -> Grid:
        bounds = Bounds.from_inp(bounds)
        # I <3 axis orders!
        (minrow, maxrow), (mincol, maxcol) = rowcol(
            self.transform, [bounds.left, bounds.right], [bounds.top, bounds.bottom]
        )
        width = maxcol - mincol
        height = maxrow - minrow
        if width < 0 or height < 0:  # pragma: no cover
            raise ValueError(f"bounds {bounds} are outside of source grid")
        return Grid.from_bounds(bounds, Shape(height, width), self.crs)

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
