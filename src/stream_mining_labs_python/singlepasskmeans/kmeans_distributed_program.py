from __future__ import annotations

import argparse
import itertools
import time

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, ProcessWindowFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from stream_mining_labs_python.singlepasskmeans.util.kmeans_data import CENTROIDS
from stream_mining_labs_python.singlepasskmeans.util.point_iterator import PointIterator


def _throttle_and_pass(value, delay_ms: float):
    if delay_ms > 0:
        time.sleep(delay_ms / 1000.0)
    return value


def _initial_centroids(k: int) -> list[tuple[float, float]]:
    if k <= 0:
        raise ValueError("k must be positive")
    if k > len(CENTROIDS):
        raise ValueError(f"k must be <= {len(CENTROIDS)} for this demo program")
    return [(float(x), float(y)) for _, x, y in CENTROIDS[:k]]


def _nearest_cluster(x: float, y: float, centroids: list[tuple[float, float]]) -> int:
    best_idx = 0
    best_dist = float("inf")
    for i, (cx, cy) in enumerate(centroids):
        d = (x - cx) ** 2 + (y - cy) ** 2
        if d < best_dist:
            best_idx = i
            best_dist = d
    return best_idx


def _assign_cluster(point_tuple, centroids: list[tuple[float, float]]):
    x, y, w = point_tuple
    cluster_id = _nearest_cluster(float(x), float(y), centroids)
    # Emit partial ClusteringFeature payload: (cluster_id, n, ls_x, ls_y, ss_x, ss_y)
    return cluster_id, int(w), float(x), float(y), float(x) * float(x), float(y) * float(y)


class ClusterAggregateWindow(ProcessWindowFunction):
    """Aggregates partial CF payloads for a single cluster key."""

    def process(self, key, context, elements):
        total_n = 0
        total_ls_x = 0.0
        total_ls_y = 0.0
        total_ss_x = 0.0
        total_ss_y = 0.0

        for cluster_id, n, ls_x, ls_y, ss_x, ss_y in elements:
            total_n += int(n)
            total_ls_x += float(ls_x)
            total_ls_y += float(ls_y)
            total_ss_x += float(ss_x)
            total_ss_y += float(ss_y)

        yield int(key), total_n, total_ls_x, total_ls_y, total_ss_x, total_ss_y


class GlobalModelCoordinator(KeyedProcessFunction):
    """
    Maintains one global list of cluster features.

    Upstream cluster aggregation is distributed. This coordinator runs on a single key
    to assemble a global model snapshot that is easy to inspect in demos.
    """

    def __init__(self, k: int) -> None:
        self.k = k

    def open(self, runtime_context: RuntimeContext) -> None:
        # Keyed ValueState keeps one model snapshot for key=0, managed by Flink.
        self.model_state = runtime_context.get_state(
            ValueStateDescriptor(
                "global-clustering-model",
                Types.LIST(
                    Types.TUPLE(
                        [
                            Types.INT(),
                            Types.LONG(),
                            Types.FLOAT(),
                            Types.FLOAT(),
                            Types.FLOAT(),
                            Types.FLOAT(),
                        ]
                    )
                ),
            )
        )

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        cluster_id, n, ls_x, ls_y, ss_x, ss_y = value

        model = self.model_state.value()
        if model is None:
            model = [
                (i, 0, 0.0, 0.0, 0.0, 0.0)
                for i in range(self.k)
            ]

        # Replace only the updated cluster slot; keep other clusters untouched.
        model[int(cluster_id)] = (
            int(cluster_id),
            int(n),
            float(ls_x),
            float(ls_y),
            float(ss_x),
            float(ss_y),
        )
        self.model_state.update(model)

        # Render current global model snapshot.
        parts = []
        for cid, cn, cls_x, cls_y, css_x, css_y in model:
            if cn > 0:
                cp_x = cls_x / cn
                cp_y = cls_y / cn
                parts.append(
                    f"[c{cid} cp ({cp_x:.2f} {cp_y:.2f}), n {cn}, "
                    f"ls ({cls_x:.2f} {cls_y:.2f}), ss ({css_x:.2f} {css_y:.2f})]"
                )
            else:
                parts.append(f"[c{cid} empty]")

        yield " ".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Distributed single-pass KMeans demo: distributed per-cluster aggregation "
            "plus a single global model coordinator"
        )
    )
    parser.add_argument("--delay", type=float, default=100.0, help="Delay between points in ms")
    parser.add_argument("--window", type=int, default=10, help="Count window per cluster")
    parser.add_argument("--k", type=int, default=3, help="Number of clusters")
    parser.add_argument("--records", type=int, default=5000)
    parser.add_argument("--parallelism", type=int, default=2)
    args = parser.parse_args()

    centroids = _initial_centroids(args.k)

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

    # Stage 1 (parallel): assign each point to nearest cluster and emit CF partials.
    partials = points.map(
        lambda p: _assign_cluster(p, centroids),
        output_type=Types.TUPLE(
            [
                Types.INT(),
                Types.INT(),
                Types.FLOAT(),
                Types.FLOAT(),
                Types.FLOAT(),
                Types.FLOAT(),
            ]
        ),
    )

    # Stage 2 (distributed): aggregate partials by cluster_id in keyed windows.
    per_cluster = (
        partials.key_by(lambda t: t[0], key_type=Types.INT())
        .count_window(args.window)
        .process(
            ClusterAggregateWindow(),
            output_type=Types.TUPLE(
                [
                    Types.INT(),
                    Types.LONG(),
                    Types.FLOAT(),
                    Types.FLOAT(),
                    Types.FLOAT(),
                    Types.FLOAT(),
                ]
            ),
        )
    )

    # Stage 3 (single coordinator): keep one global list of CFs for easy inspection.
    model = per_cluster.key_by(lambda _: 0, key_type=Types.LONG()).process(
        GlobalModelCoordinator(k=args.k),
        output_type=Types.STRING(),
    )

    model.print()
    env.execute("distributed-single-pass-kmeans")


if __name__ == "__main__":
    main()
