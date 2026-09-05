from __future__ import annotations

from dataclasses import dataclass

from .point import Point


@dataclass
class Centroid(Point):
    id: int = 0

    @classmethod
    def from_point(cls, centroid_id: int, point: Point) -> "Centroid":
        return cls(x=point.x, y=point.y, weight=point.weight, id=centroid_id)

    def __str__(self) -> str:
        return f"{self.id} {super().__str__()}"
