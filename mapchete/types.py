from collections.abc import Iterable


class Bounds(list):
    left: float = None
    bottom: float = None
    right: float = None
    top: float = None

    def __init__(self, left=None, bottom=None, right=None, top=None):
        if isinstance(left, Iterable):
            left, bottom, right, top = left
        self.left, self.bottom, self.right, self.top = left, bottom, right, top

    def __iter__(self):
        yield self.left
        yield self.bottom
        yield self.right
        yield self.top

    def __len__(self):
        return 4

    def __str__(self):
        return f"<Bounds(left={self.left}, bottom={self.bottom}, left={self.right}, left={self.top})>"

    def __getitem__(self, item):
        if isinstance(item, int):
            return list(self)[item]
        elif isinstance(item, str):
            return self.__getattribute__(item)

    def intersects(self, other):
        other = other if isinstance(other, Bounds) else Bounds(other)
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
