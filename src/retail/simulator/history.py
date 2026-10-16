"""Generate historical invoices for the batch path.

Writes gzipped JSON lines, one file per store per month:

    data/raw/invoices/2026-09/BLR01.jsonl.gz

Each line is an Invoice event, exactly as the live simulator sends it to Kafka,
so the bronze layer can ingest both paths with one schema. The output is
deterministic for a given --seed and end date.

    python -m retail.simulator.history --months 12
"""

import argparse
import gzip
import logging
import random
import time as clock
from collections.abc import Iterator
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from retail import config
from retail.catalog import (
    DEFAULT_HISTORY_MONTHS,
    STORE_TZ,
    Product,
    Store,
    add_months,
    historical_price,
    load_products,
    load_stores,
)
from retail.simulator.invoices import InvoiceFactory

log = logging.getLogger(__name__)

# Stores open 08:00-22:00 with a lunch bump and a bigger evening peak.
OPEN_HOURS = list(range(8, 22))
HOUR_WEIGHTS = [3, 5, 7, 8, 9, 8, 6, 5, 6, 8, 10, 11, 9, 5]
# Monday..Sunday: weekends are busier.
WEEKDAY_FACTOR = [0.9, 0.85, 0.9, 0.95, 1.1, 1.35, 1.3]


def month_starts(start: date, end: date) -> Iterator[date]:
    month = start.replace(day=1)
    while month < end:
        yield month
        month = add_months(month, 1)


def invoice_times(day: date, count: int, rng: random.Random) -> list[datetime]:
    """`count` sorted UTC timestamps within the store's trading hours on `day`."""
    times = []
    for hour in rng.choices(OPEN_HOURS, HOUR_WEIGHTS, k=count):
        local = datetime.combine(day, time(hour), STORE_TZ) + timedelta(
            seconds=rng.uniform(0, 3600)
        )
        times.append(local.astimezone(UTC))
    return sorted(times)


def generate(
    products: list[Product],
    stores: list[Store],
    start: date,
    end: date,
    invoices_per_day: int,
    out_dir: Path,
    seed: int,
) -> int:
    """Write invoices for days in [start, end). Returns the number written."""
    rng = random.Random(seed)
    factory = InvoiceFactory(products, rng)
    written = 0

    for month in month_starts(start, end):
        prices = {p.product_id: historical_price(p.price, month, end) for p in products}
        last_day = min(add_months(month, 1), end)
        for store in stores:
            first_day = max(month, start, store.opened_on)
            if first_day >= last_day:
                continue
            path = out_dir / month.strftime("%Y-%m") / f"{store.store_id}.jsonl.gz"
            path.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(path, "wt", encoding="utf-8") as f:
                day = first_day
                while day < last_day:
                    expected = (
                        invoices_per_day * store.size_factor * WEEKDAY_FACTOR[day.weekday()]
                    )
                    count = round(expected * rng.uniform(0.85, 1.15))
                    for ts in invoice_times(day, count, rng):
                        invoice = factory.build(store.store_id, ts, prices)
                        f.write(invoice.model_dump_json() + "\n")
                    written += count
                    day += timedelta(days=1)
        log.info("%s done (%d invoices so far)", month.strftime("%Y-%m"), written)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate historical invoice files.")
    parser.add_argument("--months", type=int, default=DEFAULT_HISTORY_MONTHS,
                        help="whole months back from the current month to start at")
    parser.add_argument("--invoices-per-day", type=int, default=300,
                        help="invoices per day for an average-sized store")
    parser.add_argument("--out", type=Path, default=config.DATA_DIR / "raw" / "invoices")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--overwrite", action="store_true",
                        help="delete existing files in --out first")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    existing = sorted(args.out.glob("*/*.jsonl.gz"))
    if existing:
        if not args.overwrite:
            parser.error(f"{args.out} already has {len(existing)} files; pass --overwrite")
        for path in existing:
            path.unlink()

    end = date.today()  # history stops at yesterday; today belongs to the live stream
    start = add_months(end.replace(day=1), -args.months)
    log.info("generating %s to %s into %s", start, end - timedelta(days=1), args.out)

    began = clock.monotonic()
    written = generate(load_products(), load_stores(), start, end,
                       args.invoices_per_day, args.out, args.seed)
    log.info("wrote %d invoices in %.1fs", written, clock.monotonic() - began)


if __name__ == "__main__":
    main()
