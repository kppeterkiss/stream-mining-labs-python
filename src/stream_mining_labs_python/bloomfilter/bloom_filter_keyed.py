from __future__ import annotations

from collections import defaultdict

from stream_mining_labs_python.utilities.bitmap import BitMap
from stream_mining_labs_python.utilities.set_of_hashes import SetOfHashes


class BloomFilterKeyed:
    """Keeps a separate Bloom filter state for each key.

        Note:
        - This class is retained as a reference/in-memory implementation.
        - The active PyFlink job now uses Flink-managed keyed ValueState in
            bloom_filter_program.py for checkpoint/recovery/rescaling semantics.

    Practical effect in keyed Flink pipelines:
    - key_by decides which parallel subtask owns each key.
    - inside each subtask, this class keeps an independent Bloom filter per key.
    - duplicate detection is therefore "per key" rather than "global across all keys".

        Ownership sketch (parallelism = 2, key = first character code):

                stream -> key_by(ord(value[0]))

                worker-0 owns keys {97:'a', 99:'c', ...}
                    _bitmaps[97], _hashes[97]
                    _bitmaps[99], _hashes[99]

                worker-1 owns keys {98:'b', 100:'d', ...}
                    _bitmaps[98], _hashes[98]
                    _bitmaps[100], _hashes[100]

        With one shared bitmap per worker, keys on that worker would interfere.
    """

    def __init__(self, k: int = 3, n: int = 10) -> None:
        self.k = k
        self.n = n
        # One bitmap per key. In a keyed Flink stream, records with the same key
        # are routed to the same parallel subtask, so this per-key state stays local.
        # With parallelism > 1, each worker stores only the keys assigned to it.
        # This avoids shared mutable state between workers and keeps updates isolated.
        self._bitmaps: dict[int, BitMap] = defaultdict(lambda: BitMap(n))
        # Hash functions are also per key to keep each key's Bloom filter independent.
        # If one SetOfHashes were shared for all keys, all keys would project into the
        # same hash family + bitmap space, increasing cross-key interference.
        self._hashes: dict[int, SetOfHashes] = defaultdict(lambda: SetOfHashes(k, n))

    def process_element(self, key: int, value: str) -> str | None:
        # Because Flink key_by partitions by key, parallelism scales across keys:
        # each subtask updates only the key-groups it owns, with no cross-key writes.
        bitmap = self._bitmaps[key]
        hashes = self._hashes[key].hashes
        indexes = [h.get_hash(value) for h in hashes]
        # Bloom filter decision: if any target bit is still 0, this value is treated
        # as not-seen-yet for this key; then all bits are set.
        free = any(not bitmap.get(i) for i in indexes)
        for idx in indexes:
            bitmap.set(idx)
        # Returned value means "first observed for this key" (subject to Bloom
        # false-positive behavior), None means probable duplicate for this key.
        return value if free else None
