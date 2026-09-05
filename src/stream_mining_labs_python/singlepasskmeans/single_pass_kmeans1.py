from __future__ import annotations

import random

from stream_mining_labs_python.singlepasskmeans.util.clustering_feature import ClusteringFeature
from stream_mining_labs_python.singlepasskmeans.util.point import Point


class SinglePassKMeans1:
    def __init__(self, k: int = 3, seed: int | None = None) -> None:
        self.k = k
        self.random = random.Random(seed)
        self.cfs: list[ClusteringFeature] = []

    def _unique_random_indexes(self, count: int, max_value: int) -> list[int]:
        values = list(range(max_value))
        self.random.shuffle(values)
        return values[:count]

    def process_window(self, elements: list[Point]) -> list[ClusteringFeature]:
        points = list(elements)
        assignment = [0 for _ in points]
        points.extend(cf.get_as_point() for cf in self.cfs)
        assignment.extend(0 for _ in self.cfs)

        if len(points) < self.k:
            return self.cfs

        init_indexes = self._unique_random_indexes(self.k, len(points))
        centroids = [Point(points[i].x, points[i].y, points[i].weight) for i in init_indexes]

        if not self.cfs:
            self.cfs = [ClusteringFeature() for _ in range(self.k)]

        converge = False
        while not converge:
            converge = True
            clusters: list[list[Point]] = [[] for _ in centroids]
            for j, p in enumerate(points):
                if any(c.x == p.x and c.y == p.y and c.weight == p.weight for c in centroids):
                    continue
                distances = [c.euclidean_distance(p) for c in centroids]
                closest = min(range(len(distances)), key=lambda idx: distances[idx])
                if assignment[j] != closest:
                    assignment[j] = closest
                    converge = False
                clusters[closest].append(p)

            for i, cluster in enumerate(clusters):
                cf = ClusteringFeature()
                for p in cluster:
                    cf.add(p)
                centroids[i] = cf.get_as_point()
                self.cfs[i] = cf

        return self.cfs
