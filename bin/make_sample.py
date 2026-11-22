#!/bin/python3

import argparse
import csv
import random
from pathlib import Path


def make_sample(
    input_csv: Path,
    output_csv: Path,
    sample_rate: float,
    seed: int,
    max_rows: int | None = None,
) -> tuple[int, int]:
    rng = random.Random(seed)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    sampled_rows = 0

    with (
        input_csv.open("r", encoding="utf-8", newline="") as src,
        output_csv.open("w", encoding="utf-8", newline="") as dst,
    ):
        reader = csv.reader(src)
        writer = csv.writer(dst)

        header = next(reader, None)
        if header is None:
            raise ValueError(f"Empty input CSV: {input_csv}")
        writer.writerow(header)

        for row in reader:
            total_rows += 1
            if rng.random() < sample_rate:
                writer.writerow(row)
                sampled_rows += 1
                if max_rows is not None and sampled_rows >= max_rows:
                    break

    return total_rows, sampled_rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a deterministic sample CSV from full NYC TLC dataset."
    )
    parser.add_argument("--input", required=True, type=Path, help="Path to full CSV")
    parser.add_argument("--output", required=True, type=Path, help="Path to sample CSV")
    parser.add_argument(
        "--rate",
        type=float,
        default=0.05,
        help="Sampling probability per row"
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional hard cap on sampled rows",
    )
    args = parser.parse_args()

    total, sampled = make_sample(
        input_csv=args.input,
        output_csv=args.output,
        sample_rate=args.rate,
        seed=args.seed,
        max_rows=args.max_rows,
    )
    print(f"Processed {total} rows, wrote {sampled} sampled rows to {args.output}")


if __name__ == "__main__":
    main()