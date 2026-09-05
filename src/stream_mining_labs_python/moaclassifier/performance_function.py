from __future__ import annotations


class PerformanceFunction:
    @staticmethod
    def aggregate(values: list[bool]) -> float:
        if not values:
            return 0.0
        return sum(1 if v else 0 for v in values) / len(values)
