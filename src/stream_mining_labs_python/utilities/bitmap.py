from __future__ import annotations


class BitMap:
    """Integer-backed bitmap/counter array used across sketches."""

    def __init__(self, n: int) -> None:
        if n <= 0:
            raise ValueError("bitmap length must be positive")
        self.map = [0] * n

    def set(self, pos: int) -> None:
        self.map[pos] = 1

    def add(self, pos: int) -> None:
        self.map[pos] += 1

    def get(self, pos: int) -> bool:
        return self.map[pos] > 0

    def get_value(self, pos: int) -> int:
        return self.map[pos]

    def get_length(self) -> int:
        return len(self.map)

    def __repr__(self) -> str:
        return str(self.map)
