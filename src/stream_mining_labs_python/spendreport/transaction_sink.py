import socket

from pyflink.datastream.functions import SinkFunction, MapFunction


class TransactionPrinterSink(SinkFunction):
    """
    A custom PyFlink SinkFunction that mirrors the structure of a SourceFunction.
    It receives processed streams and prints transactions to standard output.
    """

    def open(self, context):
        # Triggered once when the task starts running on a TaskManager slot.
        # Ideal place to initialize database connections, HTTP clients, or file handles.
        pass

    def invoke(self, value, context):
        # Triggered automatically for EVERY single incoming record in the stream.
        # 'value' contains the processed record (e.g., a Transaction object).
        print(f"[SINK OUTPUT] Processing Result: {value}")

    def close(self):
        # Triggered once when the stream is completed or stopped.
        # Used to clean up and flush connections gracefully.
        pass

class FlexiblePythonSinkMap(MapFunction):
    """
    A custom MapFunction acting as a flexible Python-native Sink.
    Supported modes: 'print', 'file', 'socket'
    """

    def __init__(self, mode='print', file_path=None, hostname=None, port=None):
        self.mode = mode.lower()
        self.file_path = file_path
        self.hostname = hostname
        self.port = port

        # Kapcsolatok / fájlkezelők belső változói
        self.file_handle = None
        self.socket_handle = None

    def open(self, context):
        """Ez a metódus egyszer fut le, amikor a TaskManager slot elindul"""
        if self.mode == 'file':
            if not self.file_path:
                raise ValueError("File path must be provided for 'file' mode.")
            # A 'a' mód miatt folyamatosan hozzáír a fájlhoz (append)
            self.file_handle = open(self.file_path, 'a', encoding='utf-8')

        elif self.mode == 'socket':
            if not self.hostname or not self.port:
                raise ValueError("Hostname and port must be provided for 'socket' mode.")
            self.socket_handle = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket_handle.connect((self.hostname, self.port))

    def map(self, value):
        """Minden egyes beérkező stream-elemen lefut"""
        output_str = f"{value}\n"

        if self.mode == 'print':
            print(f"[FLEX SINK] {value}")

        elif self.mode == 'file' and self.file_handle:
            self.file_handle.write(output_str)
            self.file_handle.flush()  # Azonnal kiírjuk a lemezre

        elif self.mode == 'socket' and self.socket_handle:
            self.socket_handle.sendall(output_str.encode('utf-8'))

        return value  # Visszaadjuk az elemet a Flink stream folyamatosságáért

    def close(self):
        """A pipeline leállásakor lezárja az erőforrásokat"""
        if self.file_handle:
            self.file_handle.close()
        if self.socket_handle:
            self.socket_handle.close()

