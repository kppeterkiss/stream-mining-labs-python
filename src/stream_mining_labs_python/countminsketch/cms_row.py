from __future__ import annotations

from stream_mining_labs_python.utilities.bitmap import BitMap
from stream_mining_labs_python.utilities.simple_hash_function import SimpleHashFunction


class CMSRow:
    def __init__(self, n: int) -> None:
        self.bm = BitMap(n)
        self.hash = SimpleHashFunction(n)

    def hash_value(self, value: str) -> None:
        self.bm.add(self.hash.get_hash(value))

    def query(self, value: str) -> int:
        return self.bm.get_value(self.hash.get_hash(value))
