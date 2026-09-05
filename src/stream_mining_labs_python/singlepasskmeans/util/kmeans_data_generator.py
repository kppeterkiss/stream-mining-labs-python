from __future__ import annotations

import argparse
import random
from pathlib import Path


def uniform_random_centers(rnd: random.Random, num: int, dimensionality: int, value_range: float):
    half_range = value_range / 2
    points = []
    for _ in range(num):
        points.append([(rnd.random() * value_range) - half_range for _ in range(dimensionality)])
    return points


def write_point(coordinates: list[float]) -> str:
    return " ".join(f"{c:.2f}" for c in coordinates)


def write_center(center_id: int, coordinates: list[float]) -> str:
    return f"{center_id} {write_point(coordinates)}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate KMeans data")
    parser.add_argument("-points", type=int, required=True)
    parser.add_argument("-k", type=int, required=True)
    parser.add_argument("-output", default="/tmp")
    parser.add_argument("-stddev", type=float, default=0.08)
    parser.add_argument("-range", type=float, default=100.0, dest="value_range")
    parser.add_argument("-seed", type=int, default=4650285087650871364)
    args = parser.parse_args()

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    rnd = random.Random(args.seed)
    means = uniform_random_centers(rnd, args.k, 2, args.value_range)

    points_path = out_dir / "points"
    centers_path = out_dir / "centers"

    absolute_stddev = args.stddev * args.value_range
    rows = []
    next_centroid = 0
    for _ in range(args.points):
        centroid = means[next_centroid]
        point = [rnd.gauss(centroid[dim], absolute_stddev) for dim in range(2)]
        rows.append(write_point(point))
        next_centroid = (next_centroid + 1) % args.k
    points_path.write_text("\n".join(rows) + "\n", encoding="utf-8")

    centers = uniform_random_centers(rnd, args.k, 2, args.value_range)
    center_rows = [write_center(i + 1, center) for i, center in enumerate(centers)]
    centers_path.write_text("\n".join(center_rows) + "\n", encoding="utf-8")

    print(f"Wrote {args.points} data points to {points_path}")
    print(f"Wrote {args.k} cluster centers to {centers_path}")


if __name__ == "__main__":
    main()
