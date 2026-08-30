"""Public CLI for the reproducible supply-chain engineering case."""

from __future__ import annotations

import argparse
import json

from .pipeline import run_demo_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate, process, validate, aggregate, and publish the demo dataset.")
    parser.add_argument("--scale", choices=("smoke", "portfolio"), default="portfolio")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", default="site")
    parser.add_argument(
        "--database",
        choices=("mysql", "skip"),
        default="mysql",
        help="Use MySQL for the round-trip, or skip only for fast offline tests.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    result = run_demo_pipeline(args.scale, args.seed, args.output, args.database)
    print(json.dumps({"state": "PASS", "source_rows": result["source_rows"], "output": args.output}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
