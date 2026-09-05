from __future__ import annotations

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.data_stream import DataStream


def get_text_stream(
    env: StreamExecutionEnvironment,
    source: str,
    *,
    host: str,
    port: int,
    input_path: str,
    delimiter: str = "\n",
) -> DataStream:
    """Returns a text stream from file or socket for PyFlink jobs.

    Some PyFlink versions do not expose socket_text_stream in Python API.
    In that case we call the underlying Java environment method and wrap it.
    """
    if source == "file":
        print(' *** using file source *** ')
        return env.read_text_file(input_path)

    if hasattr(env, "socket_text_stream"):
        print(' *** using file source *** ')

        return env.socket_text_stream(host, port, delimiter)

    # Fallback for PyFlink versions where Python API lacks socket_text_stream:
    # call the Java StreamExecutionEnvironment socketTextStream(...) directly,
    # then wrap the returned Java stream as a Python DataStream.
    j_env = env._j_stream_execution_environment
    j_stream = j_env.socketTextStream(host, int(port), delimiter)
    return DataStream(j_stream)
