from collections.abc import Iterable


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

    def __init__(self, left=None, bottom=None, right=None, top=None, strict=True):
        self.left, self.bottom, self.right, self.top = left, bottom, right, top
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

    @classmethod
    def from_inp(cls, inp, strict=True):
        if isinstance(inp, (list, tuple)):
            if len(inp) != 4:
                raise ValueError("Bounds must be initialized with exactly four values.")
            return Bounds(*inp, strict=strict)
        elif isinstance(inp, dict):
            return Bounds.from_dict(inp, strict=strict)
        else:
            raise TypeError(f"cannot create Bounds using {inp}")

    @staticmethod
    def from_dict(inp, strict=True):
        return Bounds(**inp, strict=strict)

    def to_dict(self) -> dict:
        """Return dictionary representation."""
        return {
            "left": self.left,
            "bottom": self.bottom,
            "right": self.right,
            "top": self.top,
        }

    def intersects(self, other) -> bool:
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

    def __init__(self, min=None, max=None, descending=False):
        self.min, self.max = min, max
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

    @classmethod
    def from_inp(cls, min=None, max=None, descending=False):
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
    def from_int(inp, **kwargs):
        return ZoomLevels(min=inp, max=inp, **kwargs)

    @staticmethod
    def from_list(inp, **kwargs):
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
    def from_dict(inp, **kwargs):
        try:
            return ZoomLevels(min=inp["min"], max=inp["max"], **kwargs)
        except KeyError:
            raise KeyError(f"dict does not contain 'min' and 'max' keys: {inp}")

    def to_dict(self) -> dict:
        return {
            "min": self.min,
            "max": self.max,
        }

    def intersection(self, other) -> "ZoomLevels":
        other = other if isinstance(other, ZoomLevels) else ZoomLevels(other)
        intersection = set(self).intersection(set(other))
        if len(intersection) == 0:
            raise ValueError("ZoomLevels do not intersect")
        return ZoomLevels(min(intersection), max(intersection))

    def intersects(self, other) -> bool:
        other = other if isinstance(other, ZoomLevels) else ZoomLevels(other)
        try:
            return len(self.intersection(other)) > 0
        except ValueError:
            return False

    def descending(self) -> "ZoomLevels":
        return ZoomLevels(min=self.min, max=self.max, descending=True)
