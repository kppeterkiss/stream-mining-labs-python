from __future__ import annotations

from dataclasses import dataclass
from math import sqrt


@dataclass
class Point:
    x: float = 0.0
    y: float = 0.0
    weight: int = 1

    def add(self, other: "Point") -> "Point":
        self.x += other.x
        self.y += other.y
        return self

    def div(self, val: int) -> "Point":
        self.x /= val
        self.y /= val
        return self

    def euclidean_distance(self, other: "Point") -> float:
        return sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)

    def clear(self) -> None:
        self.x = 0.0
        self.y = 0.0

    def __str__(self) -> str:
        return f"{self.x:.2f} {self.y:.2f}"
