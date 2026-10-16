"""Idempotency of the live-store writes, against a throwaway Postgres database."""

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest

from retail.streaming.live_store import PriceChange, Receipt, SaleLine, apply_batch

pytestmark = pytest.mark.postgres

DAY = date(2026, 10, 16)
NOW = datetime(2026, 10, 16, 10, 0, tzinfo=UTC)


def stock(conn, store="BLR01", product="P0001") -> int:
    return conn.execute(
        "SELECT stock_qty FROM inventory_current WHERE store_id = %s AND product_id = %s",
        (store, product),
    ).fetchone()[0]


def line(invoice_id="inv-1", line_no=1, product="P0001", qty=2) -> SaleLine:
    return SaleLine(invoice_id, line_no, "BLR01", product, qty, Decimal("28.00"), DAY)


def test_sales_apply_once(pg_dsn):
    with psycopg.connect(pg_dsn) as conn:
        before = stock(conn)
        batch = [line(), line(line_no=2, product="P0002", qty=1), line("inv-2", qty=3)]

        first = apply_batch(conn, batch, [], [])
        replay = apply_batch(conn, batch, [], [])

        assert (first.lines_applied, first.lines_skipped) == (3, 0)
        assert (replay.lines_applied, replay.lines_skipped) == (0, 3)
        assert stock(conn) == before - 5
        units, revenue = conn.execute(
            "SELECT units, revenue FROM live_sales_daily"
            " WHERE sale_date = %s AND store_id = 'BLR01' AND product_id = 'P0001'", (DAY,)
        ).fetchone()
        assert (units, revenue) == (5, Decimal("140.00"))


def test_duplicate_line_within_one_batch_counts_once(pg_dsn):
    with psycopg.connect(pg_dsn) as conn:
        before = stock(conn)
        result = apply_batch(conn, [line(qty=4), line(qty=4)], [], [])
        assert result.lines_applied == 1
        assert stock(conn) == before - 4


def test_stock_may_go_negative(pg_dsn):
    with psycopg.connect(pg_dsn) as conn:
        before = stock(conn)
        apply_batch(conn, [line(qty=before + 7)], [], [])
        assert stock(conn) == -7


def test_receipts_apply_once(pg_dsn):
    with psycopg.connect(pg_dsn) as conn:
        before = stock(conn)
        receipt = Receipt("r-1", "BLR01", "P0001", 50, NOW)
        assert apply_batch(conn, [], [receipt, receipt], []).receipts_applied == 1
        assert apply_batch(conn, [], [receipt], []).receipts_skipped == 1
        assert stock(conn) == before + 50


def test_price_changes_are_ordered_and_idempotent(pg_dsn):
    later = datetime(2026, 10, 16, 11, 0, tzinfo=UTC)
    older = PriceChange("P0001", Decimal("29.00"), NOW)
    newer = PriceChange("P0001", Decimal("31.00"), later)
    with psycopg.connect(pg_dsn) as conn:
        # Out of order within a batch: applied in effective_at order.
        assert apply_batch(conn, [], [], [newer, older]).prices_applied == 2
        # Replays, and changes older than the price in effect, are skipped.
        assert apply_batch(conn, [], [], [newer, older]).prices_skipped == 2

        current = conn.execute(
            "SELECT current_price FROM products WHERE product_id = 'P0001'").fetchone()[0]
        assert current == Decimal("31.00")
        rows = conn.execute(
            "SELECT price, valid_from, valid_to FROM product_prices"
            " WHERE product_id = 'P0001' ORDER BY valid_from"
        ).fetchall()
        assert [r[0] for r in rows[-3:]] == [Decimal("28.00"), Decimal("29.00"), Decimal("31.00")]
        assert sum(r[2] is None for r in rows) == 1
        for a, b in zip(rows, rows[1:], strict=False):
            assert a[2] == b[1]  # contiguous


def test_failed_batch_leaves_nothing_behind(pg_dsn):
    with psycopg.connect(pg_dsn) as conn:
        before = stock(conn)
        bad_receipt = Receipt("r-bad", "NOPE", "P0001", 5, NOW)  # unknown store: FK violation
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            apply_batch(conn, [line()], [bad_receipt], [])
        assert stock(conn) == before
        assert conn.execute("SELECT count(*) FROM processed_lines").fetchone()[0] == 0
