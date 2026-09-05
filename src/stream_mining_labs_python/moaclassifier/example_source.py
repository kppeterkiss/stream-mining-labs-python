from __future__ import annotations

import random
from typing import Generator


def example_source(seed: int = 42) -> Generator[tuple[dict[str, float], int], None, None]:
    """Infinite stream of synthetic binary classification examples."""
    rnd = random.Random(seed)
    while True:
        x1 = rnd.uniform(-1.0, 1.0)
        x2 = rnd.uniform(-1.0, 1.0)
        y = 1 if x1 + x2 + rnd.uniform(-0.2, 0.2) > 0 else 0
        yield {"x1": x1, "x2": x2}, y
