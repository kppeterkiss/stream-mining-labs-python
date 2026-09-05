from __future__ import annotations

import argparse
import itertools
import time

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessWindowFunction

from stream_mining_labs_python.singlepasskmeans.single_pass_kmeans1 import SinglePassKMeans1
from stream_mining_labs_python.singlepasskmeans.util.point import Point
from stream_mining_labs_python.singlepasskmeans.util.point_iterator import PointIterator


def _throttle_and_pass(value, delay_ms: float):
    if delay_ms > 0:
        time.sleep(delay_ms / 1000.0)
    return value


class KMeansWindowProcess(ProcessWindowFunction):
    def __init__(self, k: int = 3, seed: int | None = None) -> None:
        self._processor = SinglePassKMeans1(k=k, seed=seed)

    def process(self, key, context, elements):
        points = [Point(float(x), float(y), int(w)) for x, y, w in elements]
        cfs = self._processor.process_window(points)
        summary = "".join(str(cf) for cf in cfs)
        yield summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Single-pass KMeans practical on Apache Flink (PyFlink)")
    parser.add_argument(
        "--delay",
        type=float,
        default=100.0,
        help="Delay between points in milliseconds",
    )
    parser.add_argument("--window", type=int, default=10)
    parser.add_argument("--k", type=int, default=3)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--records", type=int, default=5000)
    parser.add_argument("--parallelism", type=int, default=1)
    args = parser.parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)

    generated_points = [
        (point.x, point.y, point.weight)
        for point in itertools.islice(PointIterator.unbounded_iter(), args.records)
    ]

    points = env.from_collection(
        generated_points,
        type_info=Types.TUPLE([Types.FLOAT(), Types.FLOAT(), Types.INT()]),
    ).map(
        lambda p: _throttle_and_pass(p, args.delay),
        output_type=Types.TUPLE([Types.FLOAT(), Types.FLOAT(), Types.INT()]),
    )

    clustering = (
        points.key_by(lambda _: 0, key_type=Types.LONG())
        .count_window(args.window)
        .process(KMeansWindowProcess(k=args.k, seed=args.seed), output_type=Types.STRING())
    )

    clustering.print()
    env.execute("single-pass-kmeans")


if __name__ == "__main__":
    main()
