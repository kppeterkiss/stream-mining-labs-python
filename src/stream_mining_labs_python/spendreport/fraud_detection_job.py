from __future__ import annotations

import argparse
import itertools
import time

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from stream_mining_labs_python.spendreport.fraud_detector import FraudDetector
from stream_mining_labs_python.spendreport.transaction_sink import TransactionPrinterSink, FlexiblePythonSinkMap
from stream_mining_labs_python.spendreport.transaction_source import (
    DEFAULT_FRAUD_PROBABILITY,
    transaction_source,
)


def _throttle_and_pass(value, delay_ms: float):
    if delay_ms > 0:
        time.sleep(delay_ms / 1000.0)
    return value


class FraudDetectorProcess(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext) -> None:
        self.small_amount = FraudDetector.SMALL_AMOUNT
        self.large_amount = FraudDetector.LARGE_AMOUNT
        self.one_minute = FraudDetector.ONE_MINUTE

        # Keyed ValueState is automatically scoped by the current key from key_by.
        # Even though this code declares one descriptor name, Flink keeps separate
        # values per account_id in the state backend.
        self.flag_state = runtime_context.get_state(
            ValueStateDescriptor("flag", Types.BOOLEAN())
        )
        # This timer timestamp is also per key, checkpointed by Flink, and restored
        # on recovery/rescaling together with the corresponding keyed state.
        self.timer_state = runtime_context.get_state(
            ValueStateDescriptor("timer-state", Types.LONG())
        )

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        account_id, amount = value
        last_transaction_was_small = self.flag_state.value()

        if last_transaction_was_small:
            if amount > self.large_amount:
                yield account_id
            timer = self.timer_state.value()
            if timer is not None:
                ctx.timer_service().delete_processing_time_timer(timer)
            self.timer_state.clear()
            self.flag_state.clear()

        if amount < self.small_amount:
            self.flag_state.update(True)
            timer = ctx.timer_service().current_processing_time() + self.one_minute
            ctx.timer_service().register_processing_time_timer(timer)
            self.timer_state.update(timer)

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext):
        self.timer_state.clear()
        self.flag_state.clear()


def main() -> None:
    parser = argparse.ArgumentParser(description="Fraud detection practical on Apache Flink (PyFlink)")
    parser.add_argument(
        "--delay",
        type=float,
        default=100.0,
        help="Delay between transactions in milliseconds",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for transaction generation",
    )
    parser.add_argument(
        "--fraud-probability",
        type=float,
        default=DEFAULT_FRAUD_PROBABILITY,
        help="Probability that a small transaction is followed by a large one",
    )
    parser.add_argument(
        "--records",
        type=int,
        default=100,
        help="Number of generated transactions to process",
    )
    parser.add_argument("--parallelism", type=int, default=1)
    args = parser.parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)

    generated_transactions = [
        (tx.account_id, tx.amount)
        for tx in itertools.islice(
            transaction_source(
                seed=args.seed,
                delay_ms=0.0,
                fraud_probability=args.fraud_probability,
            ),
            args.records,
        )
    ]
    print(generated_transactions)

    transactions = env.from_collection(
        generated_transactions,
        type_info=Types.TUPLE([Types.INT(), Types.DOUBLE()]),
    ).map(
        lambda tx: _throttle_and_pass(tx, args.delay),
        output_type=Types.TUPLE([Types.INT(), Types.DOUBLE()]),
    )


    alerts = transactions.key_by(lambda t: t[0], key_type=Types.INT()).process(
        FraudDetectorProcess(),
        output_type=Types.LONG(),
    )

    alerts.map(
        lambda account_id: f"ALERT account_id={account_id}",
        output_type=Types.STRING(),
    ).map(
        FlexiblePythonSinkMap(mode='file', file_path='alerts.txt'),
        output_type=Types.STRING()
    ).print()
    env.execute("Fraud Detection")


if __name__ == "__main__":
    main()
