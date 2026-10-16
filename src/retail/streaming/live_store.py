"""Apply a micro-batch of events to the Postgres live store, exactly once.

Spark may replay a micro-batch after a failure, so every write here is idempotent:

* invoice lines   are claimed in processed_lines (invoice_id, line_no) first; only
                  lines claimed by *this* call decrement stock and add to sales.
* stock receipts  are inserted into stock_receipts (receipt_id); only newly
                  inserted receipts add to stock.
* price changes   apply only if newer than the product's open price row, so a
                  replayed (equal) or out-of-order (older) change is skipped.

Everything for one batch runs in one transaction: either the whole batch lands,
or none of it does and the replay starts from a clean slate.

The streaming job is the only writer of inventory_current, so concurrent
transactions can't deadlock on it. With several writers you would lock the rows
in key order first (SELECT ... ORDER BY store_id, product_id FOR UPDATE).
"""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

import psycopg


@dataclass(frozen=True)
class SaleLine:
    invoice_id: str
    line_no: int
    store_id: str
    product_id: str
    qty: int
    unit_price: Decimal
    sale_date: date  # store-local date


@dataclass(frozen=True)
class Receipt:
    receipt_id: str
    store_id: str
    product_id: str
    qty: int
    received_at: datetime


@dataclass(frozen=True)
class PriceChange:
    product_id: str
    new_price: Decimal
    effective_at: datetime


@dataclass(frozen=True)
class ApplyResult:
    lines_applied: int
    lines_skipped: int
    receipts_applied: int
    receipts_skipped: int
    prices_applied: int
    prices_skipped: int


# One statement, so claiming lines, moving stock and adding sales can't drift apart.
# Data-modifying CTEs all run against the same snapshot; stock and sales both read
# the lines this statement claimed (via RETURNING), never lines claimed earlier.
APPLY_SALES = """
WITH claimed AS (
    INSERT INTO processed_lines (invoice_id, line_no)
    SELECT DISTINCT invoice_id, line_no FROM incoming_lines
    ON CONFLICT DO NOTHING
    RETURNING invoice_id, line_no
),
new_lines AS (
    SELECT DISTINCT ON (i.invoice_id, i.line_no) i.*
    FROM incoming_lines i JOIN claimed USING (invoice_id, line_no)
),
stock AS (
    UPDATE inventory_current t
    SET stock_qty = t.stock_qty - s.qty, updated_at = now()
    FROM (SELECT store_id, product_id, sum(qty) AS qty
          FROM new_lines GROUP BY store_id, product_id) s
    WHERE t.store_id = s.store_id AND t.product_id = s.product_id
),
sales AS (
    INSERT INTO live_sales_daily AS l (sale_date, store_id, product_id, units, revenue)
    SELECT sale_date, store_id, product_id, sum(qty), sum(qty * unit_price)
    FROM new_lines GROUP BY sale_date, store_id, product_id
    ON CONFLICT (sale_date, store_id, product_id) DO UPDATE
    SET units = l.units + EXCLUDED.units,
        revenue = l.revenue + EXCLUDED.revenue,
        updated_at = now()
)
SELECT count(*) FROM new_lines
"""

APPLY_RECEIPTS = """
WITH received AS (
    INSERT INTO stock_receipts (receipt_id, store_id, product_id, qty, received_at)
    SELECT DISTINCT ON (receipt_id) receipt_id, store_id, product_id, qty, received_at
    FROM incoming_receipts
    ON CONFLICT DO NOTHING
    RETURNING store_id, product_id, qty
),
stock AS (
    UPDATE inventory_current t
    SET stock_qty = t.stock_qty + r.qty, updated_at = now()
    FROM (SELECT store_id, product_id, sum(qty) AS qty
          FROM received GROUP BY store_id, product_id) r
    WHERE t.store_id = r.store_id AND t.product_id = r.product_id
)
SELECT count(*) FROM received
"""


def _apply_sales(cur: psycopg.Cursor, lines: list[SaleLine]) -> int:
    cur.execute(
        "CREATE TEMP TABLE incoming_lines (invoice_id text, line_no smallint, store_id text,"
        " product_id text, qty integer, unit_price numeric(10,2), sale_date date)"
    )
    with cur.copy("COPY incoming_lines FROM STDIN") as copy:
        for line in lines:
            copy.write_row((line.invoice_id, line.line_no, line.store_id, line.product_id,
                            line.qty, line.unit_price, line.sale_date))
    applied = cur.execute(APPLY_SALES).fetchone()[0]
    # Dropped explicitly rather than ON COMMIT DROP: if the caller already has a
    # transaction open, conn.transaction() is only a savepoint and nothing commits yet.
    cur.execute("DROP TABLE incoming_lines")
    return applied


def _apply_receipts(cur: psycopg.Cursor, receipts: list[Receipt]) -> int:
    cur.execute(
        "CREATE TEMP TABLE incoming_receipts (receipt_id text, store_id text, product_id text,"
        " qty integer, received_at timestamptz)"
    )
    with cur.copy("COPY incoming_receipts FROM STDIN") as copy:
        for r in receipts:
            copy.write_row((r.receipt_id, r.store_id, r.product_id, r.qty, r.received_at))
    applied = cur.execute(APPLY_RECEIPTS).fetchone()[0]
    cur.execute("DROP TABLE incoming_receipts")
    return applied


def _apply_price(cur: psycopg.Cursor, change: PriceChange) -> bool:
    params = {"pid": change.product_id, "at": change.effective_at, "price": change.new_price}
    cur.execute(
        "UPDATE product_prices SET valid_to = %(at)s"
        " WHERE product_id = %(pid)s AND valid_to IS NULL AND valid_from < %(at)s",
        params,
    )
    if cur.rowcount == 0:
        return False  # already applied, or older than the price in effect
    cur.execute(
        "INSERT INTO product_prices (product_id, price, valid_from)"
        " VALUES (%(pid)s, %(price)s, %(at)s)",
        params,
    )
    cur.execute("UPDATE products SET current_price = %(price)s WHERE product_id = %(pid)s", params)
    return True


def apply_batch(
    conn: psycopg.Connection,
    lines: list[SaleLine],
    receipts: list[Receipt],
    prices: list[PriceChange],
) -> ApplyResult:
    with conn.transaction(), conn.cursor() as cur:
        lines_applied = _apply_sales(cur, lines) if lines else 0
        receipts_applied = _apply_receipts(cur, receipts) if receipts else 0
        prices_applied = sum(
            _apply_price(cur, p) for p in sorted(prices, key=lambda p: p.effective_at)
        )
    return ApplyResult(
        lines_applied=lines_applied,
        lines_skipped=len(lines) - lines_applied,
        receipts_applied=receipts_applied,
        receipts_skipped=len(receipts) - receipts_applied,
        prices_applied=prices_applied,
        prices_skipped=len(prices) - prices_applied,
    )
