from __future__ import annotations

from collections.abc import Iterator

from .kmeans_data import POINTS
from .point import Point


class PointIterator(Iterator[Point]):
    def __init__(self, bounded: bool) -> None:
        self.bounded = bounded
        self.index = 0
        self.data = [Point(x, y) for (x, y) in POINTS]

    @staticmethod
    def bounded_iter() -> "PointIterator":
        return PointIterator(True)

    @staticmethod
    def unbounded_iter() -> "PointIterator":
        return PointIterator(False)

    def __iter__(self) -> "PointIterator":
        return self

    def __next__(self) -> Point:
        if self.index < len(self.data):
            value = self.data[self.index]
            self.index += 1
            return value
        if self.bounded:
            raise StopIteration
        self.index = 1
        return self.data[0]
