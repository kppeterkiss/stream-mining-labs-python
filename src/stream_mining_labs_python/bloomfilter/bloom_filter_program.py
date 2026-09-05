from __future__ import annotations

import argparse

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from stream_mining_labs_python.flink_compat import get_text_stream
from stream_mining_labs_python.utilities.set_of_hashes import SetOfHashes

class BloomFilterProcess(KeyedProcessFunction):
    def __init__(self, k: int = 3, n: int = 10) -> None:
        self.k = k
        self.n = n

    def open(self, runtime_context: RuntimeContext) -> None:
        # Flink-managed keyed state: one bitmap per current key, checkpointed and recoverable.
        self.bitmap_state = runtime_context.get_state(
            ValueStateDescriptor("bitmap", Types.LIST(Types.INT()))
        )

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        if not value:
            return

        key = int(ctx.get_current_key())
        bitmap = self.bitmap_state.value()
        if bitmap is None:
            bitmap = [0] * self.n

        # Deterministic per-key hash family; no need to persist this as state.
        hashes = SetOfHashes(self.k, self.n, seed=key).hashes
        indexes = [h.get_hash(value) for h in hashes]

        free = any(bitmap[idx] == 0 for idx in indexes)
        for idx in indexes:
            bitmap[idx] = 1

        self.bitmap_state.update(bitmap)
        if free:
            yield value


def main() -> None:
    parser = argparse.ArgumentParser(description="Bloom filter practical on Apache Flink (PyFlink)")
    parser.add_argument("--source", choices=["socket", "file"], default="file")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument(
        "--input",
        default="resources/inputEmails",
        help="Input file path for --source file",
    )
    parser.add_argument("--k", type=int, default=3)
    parser.add_argument("--n", type=int, default=10)
    parser.add_argument("--parallelism", type=int, default=1)
    args = parser.parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)

    input_stream = get_text_stream(
        env,
        args.source,
        host=args.host,
        port=args.port,
        input_path=args.input,
    )

    filtered = (
        input_stream.filter(lambda s: s is not None and len(s) > 0)
        # Partition by first-character code. All records starting with the same
        # character go to the same keyed state owner (parallel subtask).
        # This makes Bloom-filter state local per starting character while still
        # distributing different starting characters across workers.
        .key_by(lambda value: ord(value[0]), key_type=Types.LONG())
        .process(BloomFilterProcess(k=args.k, n=args.n), output_type=Types.STRING())
    )

    filtered.print()
    env.execute("Bloom filter")


if __name__ == "__main__":
    main()
