from __future__ import annotations

from stream_mining_labs_python.utilities.bitmap import BitMap
from stream_mining_labs_python.utilities.simple_hash_function import SimpleHashFunction


class ContDistinct:
    """Equivalent of the Java trailing-zero cardinality estimator."""

    def __init__(self, bitmap_length: int = 12) -> None:
        self.bitmap_length = bitmap_length
        self.bm = BitMap(bitmap_length)
        self.hash_function = SimpleHashFunction(bitmap_length)

    @staticmethod
    def get_tail_length(bin_string: str) -> int:
        tailing_zeros = 0
        for ch in reversed(bin_string):
            if ch == "0":
                tailing_zeros += 1
            else:
                return tailing_zeros
        return 0 if tailing_zeros == len(bin_string) else tailing_zeros

    def get_r(self) -> int:
        i = 0
        while i < self.bm.get_length() and not self.bm.get(i):
            i += 1
        if i == self.bm.get_length():
            return 0
        return self.bm.get_length() - i - 1

    def process_element(self, value: str) -> int:
        hv = self.hash_function.get_hash(value)
        binary = format(hv, "b")
        self.bm.set(self.bitmap_length - 1 - self.get_tail_length(binary))
        return 2 ** self.get_r()
