"""Job 2: stop the pipeline if too many raw rows were bad.

Exit code 1 makes the Airflow task fail, so bad data never reaches the metrics table.
"""
import argparse
import json
import sys

from jobs import config


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    args = p.parse_args()

    with open(f"data/quality/{args.date}.json") as f:
        ratio = json.load(f)["bad_row_ratio"]

    print(f"[quality_check] bad_row_ratio={ratio:.4f} (limit {config.MAX_BAD_ROW_RATIO})")
    if ratio > config.MAX_BAD_ROW_RATIO:
        print("FAILED: too many bad rows, not building metrics.")
        sys.exit(1)
    print("PASSED")


if __name__ == "__main__":
    main()
