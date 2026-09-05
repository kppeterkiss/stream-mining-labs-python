from __future__ import annotations

import argparse

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from stream_mining_labs_python.flink_compat import get_text_stream
from stream_mining_labs_python.utilities.set_of_hashes import SetOfHashes


class CMSProcess(KeyedProcessFunction):
    def __init__(self, row_count: int = 5, column_count: int = 12) -> None:
        self.row_count = row_count
        self.column_count = column_count

    def open(self, runtime_context: RuntimeContext) -> None:
        # Keyed ValueState: each key gets its own CMS table snapshot.
        self.table_state = runtime_context.get_state(
            ValueStateDescriptor("cms-table", Types.LIST(Types.INT()))
        )

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        if not value:
            return

        table = self.table_state.value()
        if table is None:
            table = [0] * (self.row_count * self.column_count)

        # Deterministic row hash functions per key; table remains the only persisted state.
        key = int(ctx.get_current_key())
        hashes = SetOfHashes(self.row_count, self.column_count, seed=key * 1000003).hashes

        indices = []
        for row_idx, hash_fn in enumerate(hashes):
            col_idx = hash_fn.get_hash(value)
            flat_idx = row_idx * self.column_count + col_idx
            table[flat_idx] += 1
            indices.append(flat_idx)

        self.table_state.update(table)
        yield min(table[idx] for idx in indices)


def main() -> None:
    parser = argparse.ArgumentParser(description="Count-Min Sketch practical on Apache Flink (PyFlink)")
    parser.add_argument("--source", choices=["file", "socket"], default="file")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9997)
    parser.add_argument("--input", default="resources/input")
    parser.add_argument("--rows", type=int, default=5)
    parser.add_argument("--columns", type=int, default=12)
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

    result = (
        stream.filter(lambda s: s is not None and len(s) > 0)
        .key_by(lambda value: ord(value[0]), key_type=Types.LONG())
        .process(
            CMSProcess(row_count=args.rows, column_count=args.columns),
            output_type=Types.INT(),
        )
    )

    result.print()
    env.execute("count-min sketch")


if __name__ == "__main__":
    main()
