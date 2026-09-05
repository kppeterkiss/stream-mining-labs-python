from __future__ import annotations

import argparse

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from stream_mining_labs_python.flink_compat import get_text_stream
from stream_mining_labs_python.utilities.simple_hash_function import SimpleHashFunction


class CountDistinctProcess(KeyedProcessFunction):
    def __init__(self, bitmap_length: int = 12) -> None:
        self.bitmap_length = bitmap_length

    def open(self, runtime_context: RuntimeContext) -> None:
        # Keyed ValueState: Flink stores one bitmap per current key.
        self.bitmap_state = runtime_context.get_state(
            ValueStateDescriptor("count-distinct-bitmap", Types.LIST(Types.INT()))
        )

    @staticmethod
    def _tail_zeros(bin_string: str) -> int:
        zeros = 0
        for ch in reversed(bin_string):
            if ch == "0":
                zeros += 1
            else:
                return zeros
        return 0 if zeros == len(bin_string) else zeros

    @staticmethod
    def _estimate_from_bitmap(bitmap: list[int]) -> int:
        i = 0
        while i < len(bitmap) and bitmap[i] == 0:
            i += 1
        if i == len(bitmap):
            return 1
        r = len(bitmap) - i - 1
        return 2 ** r

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        if not value:
            return

        bitmap = self.bitmap_state.value()
        if bitmap is None:
            bitmap = [0] * self.bitmap_length

        # Deterministic hash per key; avoids persisting hash object state.
        key = int(ctx.get_current_key())
        hash_value = SimpleHashFunction(self.bitmap_length, seed=key).get_hash(value)
        tail_len = self._tail_zeros(format(hash_value, "b"))
        bitmap[self.bitmap_length - 1 - tail_len] = 1

        self.bitmap_state.update(bitmap)
        yield self._estimate_from_bitmap(bitmap)


def main() -> None:
    parser = argparse.ArgumentParser(description="Count-distinct practical on Apache Flink (PyFlink)")
    parser.add_argument("--source", choices=["file", "socket"], default="file")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9998)
    parser.add_argument("--input", default="resources/input")
    parser.add_argument("--bitmap-length", type=int, default=12)
    parser.add_argument("--parallelism", type=int, default=1)
    args = parser.parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)

    stream = get_text_stream(
        env,
        args.source,
        host=args.host,
        port=args.port,
        input_path=args.input,
    )

    estimates = (
        stream.filter(lambda s: s is not None and len(s) > 0)
        .key_by(lambda _: 0, key_type=Types.LONG())
        .process(
            CountDistinctProcess(bitmap_length=args.bitmap_length),
            output_type=Types.INT(),
        )
    )

    estimates.map(lambda x: f"Estimate: {x}", output_type=Types.STRING()).print()
    env.execute("count distinct")


if __name__ == "__main__":
    main()
