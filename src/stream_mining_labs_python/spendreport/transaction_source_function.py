from __future__ import annotations

from pyflink.datastream.functions import SourceFunction

from stream_mining_labs_python.spendreport.transaction_source import transaction_source


class TransactionSourceFunction(SourceFunction):
    """
    PyFlink SourceFunction that generates synthetic transaction data.
    
    Generates an unbounded stream of transactions for fraud detection.
    Simulates realistic patterns: mostly normal amounts with occasional
    small amounts that might be followed by large amounts (suspicious).
    """

    def __init__(self, delay_ms: float = 100.0, seed: int = 42):
        """
        Args:
            delay_ms: Delay between transactions in milliseconds
            seed: Random seed for reproducibility
        """
        super().__init__()
        self.delay_ms = delay_ms
        self.seed = seed

    def run(self, ctx: SourceFunction.SourceContext) -> None:
        """Generate and emit transactions to the stream."""
        for transaction in transaction_source(seed=self.seed, delay_ms=self.delay_ms):
            ctx.collect(transaction)

    def cancel(self) -> None:
        """Handle cancellation."""
        pass
