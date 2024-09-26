from __future__ import annotations

from typing import List, Union, Optional

from mapchete.types import ZoomLevelsLike


class ZoomLevels(list):
    min: int
    max: int

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
        if isinstance(minlevel, list):
            zoom_list = [i for i in minlevel]
            self.min = min(zoom_list)
            self.max = max(zoom_list)
        elif maxlevel is not None:
            self.min, self.max = minlevel, maxlevel
        else:  # pragma: no cover
            raise TypeError(f"cannot determine zoomlevels with {(minlevel, maxlevel)}")

    @staticmethod
    def from_inp(
        min: ZoomLevelsLike, max: Optional[int] = None, descending: bool = False
    ) -> ZoomLevels:
        """Constructs ZoomLevels from various input forms"""
        if isinstance(min, int) and max is None:
            return ZoomLevels.from_int(min, descending=descending)
        elif isinstance(min, ZoomLevels) and max is None:
            if min.is_descending == descending:
                return min
            else:
                return ZoomLevels(min=min.min, max=min.max, descending=descending)
        elif isinstance(min, list) and max is None:
            return ZoomLevels.from_list(min, descending=descending)
        elif isinstance(min, dict) and max is None:
            return ZoomLevels.from_dict(min, descending=descending)
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
        other = other if isinstance(other, ZoomLevels) else ZoomLevels.from_inp(other)
        intersection = set(self).intersection(set(other))
        if len(intersection) == 0:
            raise ValueError("ZoomLevels do not intersect")
        return ZoomLevels(min(intersection), max(intersection))

    def difference(self, other: ZoomLevelsLike) -> ZoomLevels:
        other = other if isinstance(other, ZoomLevels) else ZoomLevels.from_inp(other)
        difference = set(self).difference(set(other))
        if len(difference) == 0:  # pragma: no cover
            raise ValueError("ZoomLevels do not differ")
        return ZoomLevels(min(difference), max(difference))

    def intersects(self, other: ZoomLevelsLike) -> bool:
        other = other if isinstance(other, ZoomLevels) else ZoomLevels.from_inp(other)
        try:
            return len(self.intersection(other)) > 0
        except ValueError:
            return False

    def descending(self) -> ZoomLevels:
        return ZoomLevels(min=self.min, max=self.max, descending=True)
