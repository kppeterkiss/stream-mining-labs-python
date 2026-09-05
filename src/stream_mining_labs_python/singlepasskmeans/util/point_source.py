from __future__ import annotations

import time
from collections.abc import Generator

from .point import Point
from .point_iterator import PointIterator


def point_source(delay_sec: float = 0.1) -> Generator[Point, None, None]:
    for p in PointIterator.unbounded_iter():
        time.sleep(delay_sec)
        yield p
