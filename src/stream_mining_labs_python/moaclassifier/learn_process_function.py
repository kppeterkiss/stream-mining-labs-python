from __future__ import annotations


class LearnProcessFunction:
    """Incremental trainer that emits model snapshots every N samples."""

    def __init__(self, classifier, update_frequency: int) -> None:
        self.classifier = classifier
        self.update_frequency = update_frequency
        self.number_of_examples_seen = 0

    def process_element(self, example: tuple[dict[str, float], int]):
        features, label = example
        self.classifier.learn_one(features, label)
        self.number_of_examples_seen += 1
        if self.number_of_examples_seen % self.update_frequency == 0:
            return self.classifier
        return None
