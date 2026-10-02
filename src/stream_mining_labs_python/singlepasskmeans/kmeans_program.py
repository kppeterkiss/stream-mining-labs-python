import time
import math
import itertools
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
from typing import Iterable


# 1. DATA GENERATOR
def generate_raw_points(records_limit=100):
    base_points = [
        (10.2, 9.8), (9.5, 10.5), (12.1, 11.2),  # 1. klaszter
        (95.4, 98.1), (102.3, 100.5), (98.1, 99.0)  # 2. klaszter
    ]
    return list(itertools.islice(itertools.cycle(base_points), records_limit))


def throttle_points(point, delay_s=0.05):
    if delay_s > 0:
        time.sleep(delay_s)
    return point


# 2. K_MEANS
class StreamingKMeansProcessor(ProcessWindowFunction):
    def __init__(self, k=2, decay=0.7):
        self.k = k
        self.decay = decay
        self.centroids_state = None

    def open(self, context):
        # new way for ValueState definition: saferfor  Java conversion
        state_descriptor = ValueStateDescriptor(
            "centroids",
            Types.PICKLED_BYTE_ARRAY()
        )
        self.centroids_state = context.get_state(state_descriptor)

    def process(self, key, context, elements: Iterable) -> Iterable:
        # Read ValueState
        current_centroids = self.centroids_state.value()
        if current_centroids is None:
            current_centroids = [(0.0, 0.0), (100.0, 100.0)]  # Kezdőpontok

        points = list(elements)

        # E-step: Assigning points to  centroids
        assignments = {i: [] for i in range(self.k)}
        for px, py in points:
            best_idx = -1
            min_dist = float('inf')
            for i, (cx, cy) in enumerate(current_centroids):
                dist = math.sqrt((px - cx) ** 2 + (py - cy) ** 2)
                if dist < min_dist:
                    min_dist = dist
                    best_idx = i
            assignments[best_idx].append((px, py))

        # M-step: Centroid refreshment with decay
        next_centroids = []
        for i in range(self.k):
            cx, cy = current_centroids[i]
            batch_points = assignments[i]

            if len(batch_points) > 0:
                batch_mean_x = sum(p[0] for p in batch_points) / len(batch_points)
                batch_mean_y = sum(p[1] for p in batch_points) / len(batch_points)

                new_x = cx * self.decay + batch_mean_x * (1.0 - self.decay)
                new_y = cy * self.decay + batch_mean_y * (1.0 - self.decay)
            else:
                new_x, new_y = cx, cy

            next_centroids.append((new_x, new_y))

        # Saving into ValueState
        self.centroids_state.update(next_centroids)

        window_end = context.current_processing_time()
        output_str = f"[Window End: {window_end}] Centroids: 0->({next_centroids[0][0]:.2f}, {next_centroids[0][1]:.2f}) | 1->({next_centroids[1][0]:.2f}, {next_centroids[1][1]:.2f}) [Batch size: {len(points)}]"

        return [output_str]


def run_kmeans():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    raw_data = generate_raw_points(records_limit=150)
    stream = env.from_collection(
        raw_data,
        type_info=Types.TUPLE([Types.DOUBLE(), Types.DOUBLE()])
    )

    throttled_stream = stream.map(
        lambda pt: throttle_points(pt, delay_s=0.05),
        output_type=Types.TUPLE([Types.DOUBLE(), Types.DOUBLE()])
    )

    keyed_stream = throttled_stream.key_by(lambda x: 0, key_type=Types.INT())

    #
    #windowed_stream = keyed_stream.window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
    windowed_stream = keyed_stream.count_window(size=20)
    result = windowed_stream.process(
        StreamingKMeansProcessor(k=2, decay=0.7),
        output_type=Types.STRING()
    )

    result.print()
    env.execute("Real Streaming K-Means Execution")


if __name__ == "__main__":
    run_kmeans()
