from __future__ import annotations

import math
from typing import Tuple

from affine import Affine
from rasterio.transform import array_bounds, from_bounds
from rasterio.windows import from_bounds as window_from_bounds
from rasterio.windows import bounds as window_bounds
from rasterio.windows import Window
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

        # use rasterio.window.Window to help with calculation
        window = window_from_bounds(*bounds, transform=self.transform)

        # now, properly round the window with the intention that every "touched"
        # row or column is included and that height or width cannot be 0
        window = Window(
            col_off=math.floor(window.col_off),  # type: ignore
            row_off=math.floor(window.row_off),  # type: ignore
            width=math.ceil(window.width),  # type: ignore
            height=math.ceil(window.height),  # type: ignore
        )

        return Grid.from_bounds(
            window_bounds(window, self.transform),
            Shape(window.height, window.width),
            self.crs,
        )

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
