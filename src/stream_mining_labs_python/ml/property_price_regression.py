import time
import itertools
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from river import linear_model, preprocessing, metrics


def generate_property_stream(records_limit=100):
    # (sqm, rooms, price)
    base_data = [
        (40.0, 1, 120000.0), (65.0, 2, 195000.0), (85.0, 3, 260000.0),
        (120.0, 4, 380000.0), (30.0, 1, 95000.0), (150.0, 5, 480000.0)
    ]
    return list(itertools.islice(itertools.cycle(base_data), records_limit))


class StreamingPropertyRegressor(MapFunction):
    def open(self, context):
        # River Pipeline: Először skálázunk (StandardScaler), utána jön a regresszió
        # Ez megvédi a modellt attól, hogy a nagy számok (árak/sqm) eltorzítsák a tanulást
        self.pipeline = preprocessing.StandardScaler() | linear_model.LinearRegression()
        # Valós idejű Abszolút Hiba (MAE) követése
        self.metric = metrics.MAE()

    def map(self, value):
        time.sleep(0.05)
        sqm, rooms, price = value
        features = {"sqm": sqm, "rooms": rooms}

        # A. ONLINE INFERENCE: Árbecslés
        predicted_price = self.pipeline.predict_one(features)

        # B. ONLINE TRAINING: Súlyok frissítése és hiba kalkuláció
        self.pipeline.learn_one(features, price)
        self.metric.update(price, predicted_price)

        return (f"[REGRESSOR] Sqm: {sqm:<3} | Rooms: {rooms} | Real: ${price:,.0f} "
                f"-> Predicted: ${predicted_price:,.0f} | Current MAE: ${self.metric.get():,.2f}")


def run_regression():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    raw_data = generate_property_stream(records_limit=50)
    stream = env.from_collection(raw_data, type_info=Types.TUPLE([Types.FLOAT(), Types.INT(), Types.FLOAT()]))

    result = stream.map(StreamingPropertyRegressor(), output_type=Types.STRING())
    result.print()
    env.execute("Flink + River Streaming Regression")


if __name__ == "__main__":
    run_regression()
