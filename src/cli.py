from __future__ import annotations

import argparse
from pathlib import Path

from src.pipelines.classify_drop_zone import run_classify_drop_zone


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ETL CRM Project command line interface",
    )
    parser.add_argument(
        "command",
        help="Command to run (e.g. 'classify-drop-zone')",
    )
    args = parser.parse_args()

    if args.command == "classify-drop-zone":
        run_classify_drop_zone(datalake_root=Path("datalake"))
    else:
        print(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
