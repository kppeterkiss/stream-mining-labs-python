from __future__ import annotations

from pyflink.datastream.functions import SourceFunction

from stream_mining_labs_python.singlepasskmeans.util.point import Point
from stream_mining_labs_python.singlepasskmeans.util.point_iterator import PointIterator


class PointSourceFunction(SourceFunction):
    """
    PyFlink SourceFunction that generates synthetic points for k-means clustering.
    
    Generates an unbounded stream of 2D points with optional delays.
    Points cycle through predefined coordinates with occasional variations.
    """

    def __init__(self, delay_ms: float = 100.0):
        """
        Args:
            delay_ms: Delay between points in milliseconds
        """
        super().__init__()
        self.delay_ms = delay_ms
        self.iterator = None

    def run(self, ctx: SourceFunction.SourceContext) -> None:
        """Generate and emit points to the stream."""
        self.iterator = PointIterator.unbounded_iter()
        
        import time
        while True:
            try:
                point = next(self.iterator)
                ctx.collect(point)
                time.sleep(self.delay_ms / 1000.0)
            except StopIteration:
                break

    def cancel(self) -> None:
        """Handle cancellation."""
        pass
