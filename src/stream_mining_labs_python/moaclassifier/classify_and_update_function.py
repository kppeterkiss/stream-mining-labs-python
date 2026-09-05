from __future__ import annotations


class ClassifyAndUpdateFunction:
    def __init__(self) -> None:
        self.classifier = None

    def flat_map_data(self, example: tuple[dict[str, float], int]) -> bool:
        if self.classifier is None:
            return False
        features, label = example
        prediction = self.classifier.predict_one(features)
        return prediction == label

    def flat_map_classifier(self, classifier) -> None:
        self.classifier = classifier
