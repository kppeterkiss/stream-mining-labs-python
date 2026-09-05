from __future__ import annotations

import argparse

from stream_mining_labs_python.moaclassifier.classify_and_update_function import ClassifyAndUpdateFunction
from stream_mining_labs_python.moaclassifier.example_source import example_source
from stream_mining_labs_python.moaclassifier.learn_process_function import LearnProcessFunction
from stream_mining_labs_python.moaclassifier.performance_function import PerformanceFunction


def _build_classifier(kind: str):
    if kind == "tree":
        try:
            from river import tree

            return tree.HoeffdingTreeClassifier()
        except ImportError as exc:
            raise RuntimeError("Install river to run moaclassifier main: pip install river") from exc
    raise ValueError(f"Unsupported classifier kind: {kind}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Online classifier practical in Python")
    parser.add_argument("--n", type=int, default=10000, help="Number of generated examples")
    parser.add_argument("--train-ratio", type=float, default=0.95)
    parser.add_argument("--update-frequency", type=int, default=1000)
    parser.add_argument("--window", type=int, default=1000)
    args = parser.parse_args()

    learner = LearnProcessFunction(_build_classifier("tree"), args.update_frequency)
    evaluator = ClassifyAndUpdateFunction()

    train_cutoff = int(args.n * args.train_ratio)
    results: list[bool] = []

    for idx, example in enumerate(example_source()):
        if idx >= args.n:
            break
        if idx < train_cutoff:
            updated = learner.process_element(example)
            if updated is not None:
                evaluator.flat_map_classifier(updated)
        else:
            results.append(evaluator.flat_map_data(example))

    if not results:
        print("No test examples collected.")
        return

    window_values = results[-args.window :]
    acc = PerformanceFunction.aggregate(window_values)
    print(f"Accuracy over last {len(window_values)} predictions: {acc:.4f}")


if __name__ == "__main__":
    main()
