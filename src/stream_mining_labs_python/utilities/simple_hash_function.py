from __future__ import annotations

import random


class SimpleHashFunction:
    """Linear hash of string codepoint sum: (m*x+i) mod b."""

    def __init__(self, modulo: int, seed: int | None = None) -> None:
        if modulo <= 0:
            raise ValueError("modulo must be positive")
        rnd = random.Random(seed)
        self.modulo = modulo
        self.increment = rnd.randrange(modulo)
        self.multiplier = rnd.randrange(modulo)

    @staticmethod
    def get_int_value(value: str) -> int:
        return sum(ord(ch) for ch in value)

    def get_hash(self, value: str) -> int:
        return (self.get_int_value(value) * self.multiplier + self.increment) % self.modulo
