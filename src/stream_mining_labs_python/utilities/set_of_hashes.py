from __future__ import annotations

from .simple_hash_function import SimpleHashFunction


class SetOfHashes:
    def __init__(self, k: int, n: int, seed: int | None = None) -> None:
        self.hashes = [SimpleHashFunction(n, None if seed is None else seed + i) for i in range(k)]
