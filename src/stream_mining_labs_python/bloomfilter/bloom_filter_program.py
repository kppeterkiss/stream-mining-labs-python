from __future__ import annotations

import argparse

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import StreamFormat, FileSource
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from stream_mining_labs_python.flink_compat import get_text_stream
from stream_mining_labs_python.utilities import SimpleHashFunction
from stream_mining_labs_python.utilities.set_of_hashes import SetOfHashes

class BloomFilterProcess(KeyedProcessFunction):
    def __init__(self, k: int = 3, n: int = 10, seed: int = 42) -> None:
        self.k = k
        self.n = n
        self.seed = seed
        self.hashes = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self.bitmap_state = runtime_context.get_state(
            ValueStateDescriptor("bitmap", Types.LIST(Types.INT()))
        )
        # Initialized once per task manager / subtask worker
        self.hashes = [SimpleHashFunction(self.n, None if self.seed is None else self.seed + i) for i in range(self.k)]

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        if not value:
            return

        bitmap = self.bitmap_state.value()
        if bitmap is None:
            bitmap = [0] * self.n

        indexes = [h.get_hash(value) for h in self.hashes]
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
    parser.add_argument("--port", type=int, default=12345)
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

    file_source = FileSource.for_record_stream_format(
        StreamFormat.text_line_format(),
        args.input
    ).process_static_file_set().build()

    input_stream = env.from_source(
        source=file_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source"
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
