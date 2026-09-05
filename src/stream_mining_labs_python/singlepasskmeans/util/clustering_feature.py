from __future__ import annotations

from .point import Point


class ClusteringFeature:
    def __init__(self) -> None:
        self.n = 1
        self.ls = Point()
        self.ss = Point()

    def get_as_point(self) -> Point:
        p = self.get_centroid()
        p.weight = self.n
        return p

    def add(self, p: Point) -> None:
        self.ls.x += p.x
        self.ls.y += p.y
        self.ss.x += p.x ** 2
        self.ss.y += p.y ** 2
        self.n += p.weight

    def get_centroid(self) -> Point:
        return Point(self.ls.x / self.n, self.ls.y / self.n)

    def distance(self, p: Point) -> float:
        return self.get_centroid().euclidean_distance(p)

    def __str__(self) -> str:
        c = self.get_centroid()
        return (
            f" [ cp ({c.x:.2f} {c.y:.2f}), n {self.n},"
            f"ls ({self.ls.x:.2f} {self.ls.y:.2f}), ss ({self.ss.x:.2f} {self.ss.y:.2f})]"
        )
