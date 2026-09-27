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


# 1. ADATGENERÁTOR (Sima Python iterátor)
def generate_raw_points(records_limit=500):
    # Előre definiált jól elkülönülő pontok
    base_points = [
        (10.2, 9.8), (9.5, 10.5), (12.1, 11.2),  # 1. klaszter környéke
        (95.4, 98.1), (102.3, 100.5), (98.1, 99.0)  # 2. klaszter környéke
    ]
    # Ciklikusan ismételjük a records_limit eléréséig
    return list(itertools.islice(itertools.cycle(base_points), records_limit))


# 2. SEBESSÉGKORLÁTOZÓ (Throttler) MAP OPERÁTOR
def throttle_points(point, delay_s=0.02):
    if delay_s > 0:
        time.sleep(delay_s)
    return point


# 3. A K-MEANS ABLAKFELDOLGOZÓ MOTOR
class StreamingKMeansProcessor(ProcessWindowFunction):
    def __init__(self, k=2, decay=0.7):
        self.k = k
        self.decay = decay
        self.centroids_state = None

    def open(self, context):
        # A Flink Managed State-ben tároljuk a modellt (a centroid mátrixot)
        state_descriptor = ValueStateDescriptor(
            "centroids",
            Types.LIST(Types.TUPLE([Types.FLOAT(), Types.FLOAT()]))
        )
        self.centroids_state = context.get_state(state_descriptor)

    def process(self, key, context, elements: Iterable) -> Iterable:
        # Korábbi centroidok betöltése (vagy kezdeti értékek megadása)
        current_centroids = self.centroids_state.value()
        if current_centroids is None:
            current_centroids = [(0.0, 0.0), (100.0, 100.0)]

        points = list(elements)

        # E-lépés: Pontok hozzárendelése a legközelebbi centroidhoz
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

        # M-lépés: Súlyozott centroid frissítés (Decay factorral)
        next_centroids = []
        for i in range(self.k):
            cx, cy = current_centroids[i]
            batch_points = assignments[i]

            if len(batch_points) > 0:
                batch_mean_x = sum(p[0] for p in batch_points) / len(batch_points)
                batch_mean_y = sum(p[1] for p in batch_points) / len(batch_points)

                # Eltolás a batch felé: New = Old * decay + Batch * (1 - decay)
                new_x = cx * self.decay + batch_mean_x * (1.0 - self.decay)
                new_y = cy * self.decay + batch_mean_y * (1.0 - self.decay)
            else:
                new_x, new_y = cx, cy

            next_centroids.append((new_x, new_y))

        # Állapot frissítése a következő ablak számára
        self.centroids_state.update(next_centroids)

        window_end = context.window().end
        yield f"[Window End: {window_end}] Centroids: 0->({next_centroids[0][0]:.2f}, {next_centroids[0][1]:.2f}) | 1->({next_centroids[1][0]:.2f}, {next_centroids[1][1]:.2f}) [Batch size: {len(points)}]"


# 4. A FONTOS PIPELINE FONTOSABB LÉPÉSEI
def run_kmeans():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Megoldás a hibára: from_collection-nel olvassuk a listát
    raw_data = generate_raw_points(records_limit=600)
    stream = env.from_collection(
        raw_data,
        type_info=Types.TUPLE([Types.FLOAT(), Types.FLOAT()])
    )

    # Lassítás beiktatása .map() operátorral a streaming élményért
    throttled_stream = stream.map(
        lambda pt: throttle_points(pt, delay_s=0.02),
        output_type=Types.TUPLE([Types.FLOAT(), Types.FLOAT()])
    )

    # Dummy kulcs, hogy elérjük a ValueState-et
    keyed_stream = throttled_stream.key_by(lambda x: 0, key_type=Types.INT())

    # 3 másodperces Tumbling ablakok (mini-batchek)
    windowed_stream = keyed_stream.window(TumblingProcessingTimeWindows.of(Time.seconds(3)))

    result = windowed_stream.process(
        StreamingKMeansProcessor(k=2, decay=0.7),
        output_type=Types.STRING()
    )

    result.print()
    env.execute("Real Streaming K-Means with Collection Source")


if __name__ == "__main__":
    run_kmeans()