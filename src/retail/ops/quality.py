"""Data-quality checks across silver, gold, the live store and the stream.

    python -m retail.ops.quality          (or: make quality)

Each check is pass / warn / fail. "fail" means the data is wrong (a broken key,
numbers that don't reconcile) and exits non-zero, so a scheduler can alert;
"warn" means it needs a look (stale tables, unusual volume, stream lag). Results
are stored in Postgres (dq_results) for the dashboard's Health tab.

Lake checks read the Delta tables with DuckDB, so a run takes seconds, not a
Spark start-up.
"""

import logging
import sys
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal

import duckdb
import psycopg
from confluent_kafka import KafkaException

from retail import config
from retail.catalog import TARGET_STOCK_MULTIPLE, store_today
from retail.ops.monitoring import last_batch_at, stream_lag
from retail.streaming.check import STOCK_MISMATCHES

log = logging.getLogger(__name__)

Status = Literal["pass", "warn", "fail"]

PRICE_BAND = (0.5, 1.5)  # sale price vs list price
VOLUME_Z = 3.0  # daily invoices this many standard deviations from the norm
VOLUME_WINDOW = 28
COVERAGE_DAYS = 90
MAX_STREAM_LAG = 1_000
STREAM_IDLE = timedelta(minutes=5)


@dataclass(frozen=True)
class CheckResult:
    layer: str
    name: str
    status: Status
    observed: str
    expected: str
    details: str = ""


def _count_check(layer: str, name: str, count: int, bad: Status, details: str = "") -> CheckResult:
    return CheckResult(layer, name, "pass" if count == 0 else bad, f"{count:,}", "0",
                       details if count else "")


class Lake:
    """Delta tables as DuckDB views (one view per table that exists)."""

    TABLES = {
        "items": "silver/invoice_items", "invoices": "silver/invoices",
        "rejects": "silver/invoice_rejects", "products": "silver/products",
        "stores": "silver/stores", "sales_daily": "gold/sales_daily",
        "store_daily": "gold/store_daily",
    }

    def __init__(self, lake_dir: Path) -> None:
        self.con = duckdb.connect()
        self.con.execute("INSTALL delta; LOAD delta")
        self.missing = []
        for view, rel in self.TABLES.items():
            path = lake_dir / rel
            if (path / "_delta_log").is_dir():
                escaped = str(path).replace("'", "''")
                self.con.execute(f"CREATE VIEW {view} AS SELECT * FROM delta_scan('{escaped}')")
            else:
                self.missing.append(rel)

    def one(self, sql: str, params: list | None = None):
        return self.con.execute(sql, params or []).fetchone()


# --- silver -------------------------------------------------------------------------

def silver_null_keys(lake: Lake, today: date) -> CheckResult:
    (n,) = lake.one("""SELECT count(*) FROM items WHERE invoice_id IS NULL OR line_no IS NULL
        OR product_id IS NULL OR store_id IS NULL OR event_date IS NULL OR qty IS NULL
        OR unit_price IS NULL""")
    return _count_check("silver", "invoice items have no null keys or values", n, "fail")


def silver_known_keys(lake: Lake, today: date) -> CheckResult:
    (n,) = lake.one("""SELECT count(*) FROM items i
        WHERE i.product_id NOT IN (SELECT product_id FROM products)
           OR i.store_id NOT IN (SELECT store_id FROM stores)""")
    return _count_check("silver", "every item's product and store exist", n, "fail")


def silver_price_plausible(lake: Lake, today: date) -> CheckResult:
    low, high = PRICE_BAND
    n, example = lake.one(f"""SELECT count(*), any_value(i.product_id || ' sold at ' ||
            i.unit_price || ' vs list ' || p.list_price)
        FROM items i JOIN products p USING (product_id)
        WHERE i.unit_price NOT BETWEEN {low} * p.list_price AND {high} * p.list_price""")
    return _count_check("silver", f"sale prices within {low:.0%}-{high:.0%} of list price", n,
                        "warn", f"e.g. {example}")


def silver_rejects(lake: Lake, today: date) -> CheckResult:
    n, reasons = lake.one("""SELECT count(*), string_agg(DISTINCT reject_reason, ', ')
        FROM rejects""")
    return _count_check("silver", "no rejected invoices", n, "warn", f"reasons: {reasons}")


# --- gold ---------------------------------------------------------------------------

def gold_freshness(lake: Lake, today: date) -> CheckResult:
    (latest,) = lake.one("SELECT max(event_date) FROM store_daily")
    expected = today - timedelta(days=1)
    ok = latest is not None and latest >= expected
    return CheckResult("gold", "gold includes yesterday", "pass" if ok else "warn",
                       str(latest), f">= {expected}", "" if ok else "run `make lake`")


def gold_store_days_complete(lake: Lake, today: date) -> CheckResult:
    """Every open store has a row for every day: a gap means lost or unloaded sales."""
    (latest,) = lake.one("SELECT max(event_date) FROM store_daily WHERE event_date < ?", [today])
    if latest is None:
        return CheckResult("gold", "no missing store-days", "warn", "no data", "-")
    start = latest - timedelta(days=COVERAGE_DAYS - 1)
    n, example = lake.one("""
        SELECT count(*), any_value(s.store_id || ' on ' || d.day)
        FROM stores s
        CROSS JOIN (SELECT range::DATE AS day FROM range(?::DATE, ?::DATE + 1, INTERVAL 1 DAY)) d
        LEFT JOIN store_daily g ON g.store_id = s.store_id AND g.event_date = d.day
        WHERE d.day >= s.opened_on AND g.store_id IS NULL""", [start, latest])
    return _count_check("gold", f"no missing store-days (last {COVERAGE_DAYS} days)", n, "fail",
                        f"e.g. {example}")


def gold_volume_normal(lake: Lake, today: date) -> CheckResult:
    rows = lake.con.execute(f"""
        WITH latest AS (SELECT max(event_date) AS day FROM store_daily WHERE event_date < ?),
        stats AS (
            SELECT store_id, avg(invoices) AS mean, stddev_samp(invoices) AS sd
            FROM store_daily, latest
            WHERE event_date BETWEEN latest.day - {VOLUME_WINDOW} AND latest.day - 1
            GROUP BY store_id)
        SELECT g.store_id, g.event_date, g.invoices, round(s.mean),
               (g.invoices - s.mean) / nullif(s.sd, 0) AS z
        FROM store_daily g JOIN latest ON g.event_date = latest.day JOIN stats s USING (store_id)
        ORDER BY abs(z) DESC""", [today]).fetchall()
    if not rows:
        return CheckResult("gold", "daily invoice volume is normal", "warn", "no data", "-")
    store, day, invoices, mean, z = rows[0]
    status: Status = "warn" if z is not None and abs(z) > VOLUME_Z else "pass"
    return CheckResult("gold", "daily invoice volume is normal", status,
                       f"{store} {day}: {invoices} invoices (z={z:+.1f})",
                       f"|z| <= {VOLUME_Z} vs {VOLUME_WINDOW}-day mean ~{mean:.0f}")


def gold_reconciles(lake: Lake, today: date) -> CheckResult:
    gold, silver = lake.one("""SELECT (SELECT sum(revenue) FROM sales_daily),
                                      (SELECT sum(line_amount) FROM items)""")
    return CheckResult("gold", "gold revenue equals silver", "pass" if gold == silver else "fail",
                       f"{gold}", f"{silver}")


# --- live store and stream ----------------------------------------------------------

def live_checks(dsn: str) -> list[CheckResult]:
    with psycopg.connect(dsn) as conn:
        mismatched = len(conn.execute(STOCK_MISMATCHES,
                                      {"multiple": TARGET_STOCK_MULTIPLE}).fetchall())
        negative = conn.execute(
            "SELECT count(*) FROM inventory_current WHERE stock_qty < 0").fetchone()[0]
        bad_prices = conn.execute("""
            SELECT count(*) FROM (
                SELECT product_id, count(*) FILTER (WHERE valid_to IS NULL) AS open_rows,
                       count(*) FILTER (WHERE valid_to IS NOT NULL AND valid_to <> next_from)
                           AS gaps
                FROM (SELECT *, lead(valid_from) OVER (PARTITION BY product_id
                                                       ORDER BY valid_from) AS next_from
                      FROM product_prices) p
                GROUP BY product_id) x
            WHERE open_rows <> 1 OR gaps > 0""").fetchone()[0]
    return [
        _count_check("live", "stock = opening - sold + received", mismatched, "fail"),
        _count_check("live", "no negative stock", negative, "warn",
                     "sales recorded faster than deliveries; see the Live tab"),
        _count_check("live", "price history is contiguous with one open price", bad_prices,
                     "fail"),
    ]


def stream_checks(checkpoint: Path, now: datetime) -> list[CheckResult]:
    try:
        lags = stream_lag(checkpoint)
    except KafkaException as exc:
        return [CheckResult("stream", "Kafka reachable", "warn", "unreachable", "reachable",
                            str(exc))]
    total = sum(t.lag for t in lags)
    detail = ", ".join(f"{t.topic} {t.lag:,}" for t in lags)
    results = [CheckResult("stream", "stream keeping up with Kafka",
                           "pass" if total <= MAX_STREAM_LAG else "warn",
                           f"{total:,} messages behind", f"<= {MAX_STREAM_LAG:,}", detail)]
    last = last_batch_at(checkpoint)
    if total > 0 and (last is None or now - last > STREAM_IDLE):
        results.append(CheckResult(
            "stream", "streaming job is running", "warn",
            f"last batch {last:%d %b %H:%M} UTC" if last else "never ran",
            f"a batch within {STREAM_IDLE.seconds // 60} min while messages wait",
            "start it with `make stream`"))
    return results


LAKE_CHECKS: list[Callable[[Lake, date], CheckResult]] = [
    silver_null_keys, silver_known_keys, silver_price_plausible, silver_rejects,
    gold_freshness, gold_store_days_complete, gold_volume_normal, gold_reconciles,
]


def run_all(lake_dir: Path, reader_dsn: str, today: date | None = None,
            now: datetime | None = None, include_stream: bool = True) -> list[CheckResult]:
    today, now = today or store_today(), now or datetime.now(UTC)
    lake = Lake(lake_dir)
    if lake.missing:
        results = [CheckResult("gold", "lake tables exist", "warn", f"missing: {lake.missing}",
                               "all present", "run `make lake`")]
    else:
        results = [check(lake, today) for check in LAKE_CHECKS]
    results += live_checks(reader_dsn)
    if include_stream:
        results += stream_checks(lake_dir / "_checkpoints" / "stream_kafka", now)
    return results


def save(dsn: str, results: list[CheckResult], ran_at: datetime) -> str:
    run_id = f"{ran_at:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO dq_results (run_id, ran_at, layer, check_name, status, observed,"
            " expected, details) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            [(run_id, ran_at, r.layer, r.name, r.status, r.observed, r.expected, r.details)
             for r in results],
        )
    return run_id


ICONS = {"pass": "✅", "warn": "⚠️ ", "fail": "❌"}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ran_at = datetime.now(UTC)
    results = run_all(config.LAKE_DIR, config.POSTGRES_READER_DSN, now=ran_at)
    run_id = save(config.POSTGRES_DSN, results, ran_at)
    for r in results:
        line = f"{ICONS[r.status]} {r.layer:6} {r.name:52} {r.observed}"
        if r.status != "pass":
            line += f"  (expected {r.expected}){'; ' + r.details if r.details else ''}"
        log.info(line)
    counts = {s: sum(r.status == s for r in results) for s in ICONS}
    log.info("run %s: %d pass, %d warn, %d fail", run_id, counts["pass"], counts["warn"],
             counts["fail"])
    sys.exit(1 if counts["fail"] else 0)


if __name__ == "__main__":
    main()
