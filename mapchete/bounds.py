from __future__ import annotations

from typing import Optional, Union, Iterable

from shapely.geometry import shape

from mapchete.types import CRSLike, BoundsLike, Geometry


class Bounds(list):
    """
    Class to handle geographic bounds.
    """

    left: float
    bottom: float
    right: float
    top: float
    height: float
    width: float
    crs: Optional[CRSLike] = None

    def __init__(
        self,
        left: Union[Iterable[float], float],
        bottom: Optional[float],
        right: Optional[float],
        top: Optional[float],
        strict: bool = True,
        crs: Optional[CRSLike] = None,
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
        self.crs = crs

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

    def __add__(self, other: BoundsLike) -> Bounds:
        other = Bounds.from_inp(other)
        return Bounds(
            left=min([self.left, other.left]),
            bottom=min([self.bottom, other.bottom]),
            right=max([self.right, other.right]),
            top=max([self.top, other.top]),
        )

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
        if hasattr(left, "__iter__") and not isinstance(
            left, (float, int)
        ):  # pragma: no cover
            self.left, self.bottom, self.right, self.top = [i for i in left]
        elif (
            isinstance(left, (float, int))
            and isinstance(bottom, (float, int))
            and isinstance(right, (float, int))
            and isinstance(top, (float, int))
        ):
            self.left, self.bottom, self.right, self.top = left, bottom, right, top
        else:
            raise TypeError(
                f"cannot set Bounds values from {(left, bottom, right, top)}"
            )

    @property
    def geometry(self) -> Geometry:
        return shape(self)

    # save this for later when rewriting tests for this module
    # def latlon_geometry(
    #     self, crs: Optional[CRSLike] = None, width_threshold: float = 180.0
    # ) -> Geometry:
    #     """
    #     Will create a MultiPolygon if bounds overlap with Antimeridian.
    #     """
    #     from mapchete.geometry.latlon import transform_to_latlon

    #     crs = crs or self.crs
    #     if crs is None:
    #         raise ValueError(
    #             "crs or Bounds.crs must be set in order to generate latlon_geometry."
    #         )
    #     bounds = Bounds.from_inp(
    #         transform_to_latlon(shape(self), self.crs, width_threshold=width_threshold)
    #     )
    #     if bounds.left < -180:
    #         part1 = Bounds(-180, bounds.bottom, bounds.right, bounds.top)
    #         part2 = Bounds(bounds.left + 360, bounds.bottom, 180, bounds.top)
    #         return unary_union([shape(part1), shape(part2)])
    #     elif bounds.right > 180:
    #         part1 = Bounds(-180, bounds.bottom, bounds.right - 360, bounds.top)
    #         part2 = Bounds(bounds.left, bounds.bottom, 180, bounds.top)
    #         return unary_union([shape(part1), shape(part2)])
    #     else:
    #         return shape(bounds)

    @classmethod
    def from_inp(cls, inp: BoundsLike, strict: bool = True) -> Bounds:
        if isinstance(inp, (list, tuple)):
            if len(inp) != 4:
                raise ValueError("Bounds must be initialized with exactly four values.")
            return Bounds(*inp, strict=strict)
        elif isinstance(inp, dict):
            return Bounds.from_dict(inp, strict=strict)
        elif isinstance(inp, Geometry):
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