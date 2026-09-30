import time
import itertools
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from river import linear_model, metrics


# 1. ADATGENERÁTOR (Ciklikus szimuláció)
def generate_transaction_stream(records_limit=100):
    # (amount, distance_from_home, is_fraud)
    base_data = [
        (10.5, 1.2, 0), (15.0, 0.5, 0), (8.2, 2.5, 0),  # Legitim tranzakciók
        (999.0, 850.4, 1), (1200.0, 430.1, 1)  # Csalásgyanús tranzakciók
    ]
    return list(itertools.islice(itertools.cycle(base_data), records_limit))


# 2. SINK/PROCESSZOR FUNKCIÓ (River Online Classifier)
class StreamingFraudClassifier(MapFunction):
    def open(self, context):
        # A River online logisztikus regressziós modellje
        self.model = linear_model.LogisticRegression()
        # Valós idejű ROC-AUC metrika követés
        self.metric = metrics.ROCAUC()

    def map(self, value):
        # Lassítás a streaming élmény kedvéért
        time.sleep(0.05)

        # Tuple kicsomagolása
        amount, distance, label = value
        features = {"amount": amount, "distance": distance}
        target = bool(label)

        # A. ONLINE INFERENCE: Predikció a beérkező adaton
        prediction = self.model.predict_proba_one(features)
        fraud_probability = prediction.get(True, 0.0)

        # B. ONLINE TRAINING: A modell azonnali frissítése a látott pontból
        self.model.learn_one(features, target)
        self.metric.update(target, prediction)

        return (f"[CLASSIFIER] Amount: ${amount:<7} | Distance: {distance:<5} "
                f"-> Fraud Prob: {fraud_probability:.2f} | Current ROC-AUC: {self.metric.get():.4f}")


def run_classification():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    raw_data = generate_transaction_stream(records_limit=50)
    stream = env.from_collection(raw_data, type_info=Types.TUPLE([Types.FLOAT(), Types.FLOAT(), Types.INT()]))

    result = stream.map(StreamingFraudClassifier(), output_type=Types.STRING())
    result.print()
    env.execute("Flink + River Streaming Classification")


if __name__ == "__main__":
    run_classification()
