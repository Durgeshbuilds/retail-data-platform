"""Generate historical invoices for the batch path.

Writes gzipped JSON lines, one file per store per day, like a nightly POS export:

    data/raw/invoices/2026-09/BLR01-2026-09-14.jsonl.gz

Each line is an Invoice event, exactly as the live simulator sends it to Kafka,
so the bronze layer can ingest both paths with one schema.

A store-day's content depends only on --seed, the store and the date, and files
that already exist are skipped. Re-running on a later day therefore adds just the
new days, and never rewrites a file bronze has already ingested (bronze tracks
files by path, so a rewritten file would never be read again).

    python -m retail.simulator.history --months 12
"""

import argparse
import gzip
import logging
import random
import time as clock
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from retail import config
from retail.catalog import (
    DEFAULT_HISTORY_MONTHS,
    STORE_TZ,
    Product,
    Store,
    add_months,
    load_products,
    load_stores,
    price_in_month,
    store_today,
)
from retail.simulator.invoices import InvoiceFactory

log = logging.getLogger(__name__)

# Stores open 08:00-22:00 with a lunch bump and a bigger evening peak.
OPEN_HOURS = list(range(8, 22))
HOUR_WEIGHTS = [3, 5, 7, 8, 9, 8, 6, 5, 6, 8, 10, 11, 9, 5]
# Monday..Sunday: weekends are busier.
WEEKDAY_FACTOR = [0.9, 0.85, 0.9, 0.95, 1.1, 1.35, 1.3]


def day_file(out_dir: Path, store_id: str, day: date) -> Path:
    return out_dir / day.strftime("%Y-%m") / f"{store_id}-{day.isoformat()}.jsonl.gz"


def invoice_times(day: date, count: int, rng: random.Random) -> list[datetime]:
    """`count` sorted UTC timestamps within the store's trading hours on `day`."""
    times = []
    for hour in rng.choices(OPEN_HOURS, HOUR_WEIGHTS, k=count):
        local = datetime.combine(day, time(hour), STORE_TZ) + timedelta(
            seconds=rng.uniform(0, 3600)
        )
        times.append(local.astimezone(UTC))
    return sorted(times)


def write_store_day(
    path: Path, products: list[Product], store: Store, day: date,
    invoices_per_day: int, seed: int,
) -> int:
    rng = random.Random(f"{seed}/{store.store_id}/{day.isoformat()}")
    factory = InvoiceFactory(products, rng)
    prices = {p.product_id: price_in_month(p.price, day) for p in products}
    expected = invoices_per_day * store.size_factor * WEEKDAY_FACTOR[day.weekday()]
    count = round(expected * rng.uniform(0.85, 1.15))

    # Write to a temp name and rename, so a crash never leaves a half-written file
    # that bronze could pick up.
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".tmp")
    with gzip.open(partial, "wt", encoding="utf-8") as f:
        for ts in invoice_times(day, count, rng):
            f.write(factory.build(store.store_id, ts, prices).model_dump_json() + "\n")
    partial.rename(path)
    return count


def generate(
    products: list[Product],
    stores: list[Store],
    start: date,
    end: date,
    invoices_per_day: int,
    out_dir: Path,
    seed: int,
) -> int:
    """Write invoices for days in [start, end) that have no file yet. Returns invoices written."""
    written = 0
    day = start
    while day < end:
        for store in stores:
            path = day_file(out_dir, store.store_id, day)
            if day >= store.opened_on and not path.exists():
                written += write_store_day(path, products, store, day, invoices_per_day, seed)
        if day.day == 1 or day + timedelta(days=1) == end:
            log.info("%s done (%d invoices written so far)", day, written)
        day += timedelta(days=1)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate historical invoice files.")
    parser.add_argument("--months", type=int, default=DEFAULT_HISTORY_MONTHS,
                        help="whole months back from the current month to start at")
    parser.add_argument("--invoices-per-day", type=int, default=300,
                        help="invoices per day for an average-sized store")
    parser.add_argument("--out", type=Path, default=config.RAW_INVOICES_DIR)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    end = store_today()  # history stops at yesterday; today belongs to the live stream
    start = add_months(end.replace(day=1), -args.months)
    log.info("generating %s to %s into %s", start, end - timedelta(days=1), args.out)

    began = clock.monotonic()
    written = generate(load_products(), load_stores(), start, end,
                       args.invoices_per_day, args.out, args.seed)
    log.info("wrote %d invoices in %.1fs", written, clock.monotonic() - began)


if __name__ == "__main__":
    main()
