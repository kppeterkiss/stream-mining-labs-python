from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Transaction:
    account_id: int
    amount: float
    processing_time_ms: int


@dataclass
class Alert:
    account_id: int


class FraudDetector:
    SMALL_AMOUNT = 1.00
    LARGE_AMOUNT = 500.00
    ONE_MINUTE = 60 * 1000

    def __init__(self) -> None:
        self.flag_state: dict[int, bool] = {}
        self.timer_state: dict[int, int] = {}

    def process_element(self, transaction: Transaction) -> list[Alert]:
        alerts: list[Alert] = []
        account_id = transaction.account_id
        last_transaction_was_small = self.flag_state.get(account_id)

        if last_transaction_was_small:
            if transaction.amount > self.LARGE_AMOUNT:
                alerts.append(Alert(account_id=account_id))
            self.clean_up(account_id)

        if transaction.amount < self.SMALL_AMOUNT:
            self.flag_state[account_id] = True
            timer = transaction.processing_time_ms + self.ONE_MINUTE
            self.timer_state[account_id] = timer

        return alerts

    def on_timer(self, account_id: int) -> None:
        self.timer_state.pop(account_id, None)
        self.flag_state.pop(account_id, None)

    def clean_up(self, account_id: int) -> None:
        self.timer_state.pop(account_id, None)
        self.flag_state.pop(account_id, None)
