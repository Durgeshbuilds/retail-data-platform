"""Run the batch pipeline: dims -> bronze -> silver -> gold -> check.

    python -m retail.lake.run                      # every step
    python -m retail.lake.run gold --from-date 2026-10-01
    python -m retail.lake.run check

Every step is safe to re-run: bronze and silver only process what is new, and
gold is rebuilt.
"""

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

from retail import config
from retail.lake import bronze, checks, dims, gold, silver
from retail.lake.spark import create_spark
from retail.lake.tables import Lake

log = logging.getLogger("retail.lake")

STEPS = ["dims", "bronze", "silver", "gold", "check"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Delta Lake from raw invoices.")
    parser.add_argument("steps", nargs="*", choices=STEPS,
                        help="steps to run, in pipeline order (default: all)")
    parser.add_argument("--raw-dir", type=Path, default=config.RAW_INVOICES_DIR)
    parser.add_argument("--lake-root", type=Path, default=config.LAKE_DIR)
    parser.add_argument("--from-date", type=date.fromisoformat,
                        help="gold: only rebuild dates on or after this (YYYY-MM-DD)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)  # gateway chatter on every callback

    lake = Lake(args.lake_root)
    spark = create_spark("retail-lake")
    failed = False
    try:
        for step in [s for s in STEPS if s in (args.steps or STEPS)]:
            began = time.monotonic()
            if step == "dims":
                dims.build(spark, lake)
            elif step == "bronze":
                bronze.ingest_files(spark, lake, args.raw_dir)
            elif step == "silver":
                silver.build(spark, lake)
            elif step == "gold":
                gold.build(spark, lake, args.from_date)
            elif step == "check":
                for c in checks.reconcile(spark, lake):
                    failed |= not c.ok
                    log.info("%s %-48s expected=%s actual=%s",
                             "PASS" if c.ok else "FAIL", c.name, c.expected, c.actual)
            log.info("step %s took %.1fs", step, time.monotonic() - began)
    finally:
        spark.stop()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
