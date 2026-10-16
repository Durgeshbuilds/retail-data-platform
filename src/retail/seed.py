"""Seed the live store: stores, products, price history and opening stock.

Safe to re-run. Existing rows are left untouched, so prices and stock that the
streaming job has since changed are never overwritten.

    python -m retail.seed
"""

import argparse
import logging
from datetime import date, datetime, time
from decimal import Decimal

import psycopg

from retail import config
from retail.catalog import (
    DEFAULT_HISTORY_MONTHS,
    STORE_TZ,
    TARGET_STOCK_MULTIPLE,
    Product,
    add_months,
    historical_price,
    load_products,
    load_stores,
    reorder_level,
)

log = logging.getLogger(__name__)


def price_history(
    product: Product, today: date, months: int
) -> list[tuple[datetime, datetime | None, Decimal]]:
    """SCD2 rows (valid_from, valid_to, price), one per price change, oldest first.

    Matches the prices the history generator puts on invoices, month by month.
    """
    current_month = today.replace(day=1)
    rows: list[list] = []
    for months_ago in range(months, -1, -1):
        month = add_months(current_month, -months_ago)
        price = historical_price(product.price, month, today)
        if rows and rows[-1][2] == price:
            continue  # unchanged, so the previous row stays in effect
        rows.append([datetime.combine(month, time(), STORE_TZ), None, price])
    for older, newer in zip(rows, rows[1:], strict=False):
        older[1] = newer[0]
    return [tuple(row) for row in rows]


def seed(conn: psycopg.Connection, today: date, history_months: int) -> None:
    products = load_products()
    stores = load_stores()

    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO stores (store_id, name, city, opened_on) VALUES (%s, %s, %s, %s)"
            " ON CONFLICT DO NOTHING",
            [(s.store_id, s.name, s.city, s.opened_on) for s in stores],
        )
        cur.executemany(
            "INSERT INTO products (product_id, name, category, brand, unit, current_price)"
            " VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            [(p.product_id, p.name, p.category, p.brand, p.unit, p.price) for p in products],
        )
        cur.executemany(
            "INSERT INTO product_prices (product_id, valid_from, valid_to, price)"
            " VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            [
                (p.product_id, *row)
                for p in products
                for row in price_history(p, today, history_months)
            ],
        )
        inventory = []
        for s in stores:
            for p in products:
                level = reorder_level(p, s)
                inventory.append((s.store_id, p.product_id, level * TARGET_STOCK_MULTIPLE, level))
        cur.executemany(
            "INSERT INTO inventory_current (store_id, product_id, stock_qty, reorder_level)"
            " VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            inventory,
        )

    log.info("seeded %d stores, %d products", len(stores), len(products))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--history-months", type=int, default=DEFAULT_HISTORY_MONTHS)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with psycopg.connect(config.POSTGRES_DSN) as conn:  # commits on clean exit
        seed(conn, date.today(), args.history_months)
        for table in ("stores", "products", "product_prices", "inventory_current"):
            count = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            log.info("%-18s %6d rows", table, count)


if __name__ == "__main__":
    main()
