from __future__ import annotations

import random
import time
from collections.abc import Generator
from dataclasses import dataclass

from stream_mining_labs_python.spendreport.fraud_detector import FraudDetector


DEFAULT_SMALL_TX_PROBABILITY = 0.10
DEFAULT_FRAUD_PROBABILITY = 0.35


@dataclass  # Decorator: auto-generates methods like __init__, __repr__, and __eq__ for this data container.
class Transaction:
    """Represents a financial transaction."""
    account_id: int
    amount: float
    
    def __iter__(self):
        """Allow unpacking as (account_id, amount) tuple."""
        return iter((self.account_id, self.amount))


def transaction_source(
    seed: int = 42,
    delay_ms: float = 100.0,
    fraud_probability: float = DEFAULT_FRAUD_PROBABILITY,
) -> Generator[Transaction, None, None]:
    """
    Generates an infinite stream of synthetic transactions.
    
    Simulates random transactions across multiple accounts with varying amounts.
    Some accounts are more prone to suspicious patterns (small followed by large amounts).
    
    Args:
        seed: Random seed for reproducibility
        delay_ms: Delay between transactions in milliseconds
        fraud_probability: Chance that a generated small transaction is
            followed immediately by a large transaction for the same account
    
    Yields:
        Transaction objects with account_id and amount
    """
    if not 0.0 <= fraud_probability <= 1.0:
        raise ValueError("fraud_probability must be in range [0.0, 1.0]")

    rnd = random.Random(seed)
    account_ids = list(range(1, 11))  # Accounts 1-10
    pending_large_for_account: int | None = None
    
    while True:
        if pending_large_for_account is not None:
            # Emit a large amount for the same account as the previous small tx,
            # so the fraud detector can match the pattern in sequence.
            account_id = pending_large_for_account
            amount = rnd.uniform(FraudDetector.LARGE_AMOUNT + 0.01, 1000.0)
            pending_large_for_account = None
        else:
            account_id = rnd.choice(account_ids)

            # Mostly normal traffic, with occasional small transactions.
            if rnd.random() < DEFAULT_SMALL_TX_PROBABILITY:
                amount = rnd.uniform(0.01, FraudDetector.SMALL_AMOUNT - 0.01)
                # With configurable probability, schedule a follow-up large tx
                # on the same account in the next emitted event.
                if rnd.random() < fraud_probability:
                    pending_large_for_account = account_id
            else:
                amount = rnd.uniform(FraudDetector.SMALL_AMOUNT, FraudDetector.LARGE_AMOUNT)
        
        # yield emits one item and pauses the function; on next iteration it resumes here.
        # This is why transaction_source is a generator that can stream values over time.
        yield Transaction(account_id=account_id, amount=amount)
        time.sleep(delay_ms / 1000.0)
