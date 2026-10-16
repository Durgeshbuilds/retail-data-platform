"""Read-only data access for the dashboard.

* LiveStore  queries Postgres as retail_reader: current stock, today's sales, prices.
* GoldLake   queries the gold Delta tables with DuckDB. No Spark: a dashboard query
             should take milliseconds, not a JVM start-up.

Both return pandas DataFrames with plain float/int columns, ready to chart.
"""

from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import psycopg

from retail.catalog import STORE_TZ


def store_today() -> date:
    return datetime.now(STORE_TZ).date()


class LiveStore:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def _frame(self, sql: str, params: dict | None = None) -> pd.DataFrame:
        with psycopg.connect(self.dsn) as conn:
            cur = conn.execute(sql, params)
            return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])

    def stores(self) -> pd.DataFrame:
        return self._frame("SELECT store_id, name, city FROM stores ORDER BY store_id")

    def sales_by_store(self, day: date) -> pd.DataFrame:
        """One row per store (zeros if it has sold nothing yet)."""
        return self._frame(
            """
            SELECT s.store_id, s.name,
                   coalesce(d.invoices, 0)::int AS invoices,
                   coalesce(l.units, 0)::int AS units,
                   coalesce(l.revenue, 0)::float8 AS revenue
            FROM stores s
            LEFT JOIN live_store_daily d ON d.store_id = s.store_id AND d.sale_date = %(day)s
            LEFT JOIN (
                SELECT store_id, sum(units) AS units, sum(revenue) AS revenue
                FROM live_sales_daily WHERE sale_date = %(day)s GROUP BY store_id
            ) l ON l.store_id = s.store_id
            ORDER BY s.store_id
            """,
            {"day": day},
        )

    def top_products(self, day: date, store_ids: list[str], limit: int = 10) -> pd.DataFrame:
        return self._frame(
            """
            SELECT p.name || ' (' || p.unit || ')' AS product, p.category,
                   sum(l.units)::int AS units, sum(l.revenue)::float8 AS revenue
            FROM live_sales_daily l JOIN products p USING (product_id)
            WHERE l.sale_date = %(day)s AND l.store_id = ANY(%(stores)s)
            GROUP BY p.product_id, p.name, p.unit, p.category
            ORDER BY revenue DESC LIMIT %(limit)s
            """,
            {"day": day, "stores": store_ids, "limit": limit},
        )

    def stock_alerts(self, store_ids: list[str]) -> pd.DataFrame:
        """Items at or below their reorder level, most urgent first."""
        return self._frame(
            """
            SELECT i.store_id, p.name || ' (' || p.unit || ')' AS product, p.category,
                   i.stock_qty, i.reorder_level,
                   CASE WHEN i.stock_qty < 0 THEN 'negative'
                        WHEN i.stock_qty = 0 THEN 'out'
                        ELSE 'low' END AS status,
                   i.updated_at
            FROM inventory_current i JOIN products p USING (product_id)
            WHERE i.stock_qty <= i.reorder_level AND i.store_id = ANY(%(stores)s)
            ORDER BY i.stock_qty::float8 / greatest(i.reorder_level, 1), i.store_id
            """,
            {"stores": store_ids},
        )

    def price_changes(self, days: int = 7, limit: int = 20) -> pd.DataFrame:
        return self._frame(
            """
            SELECT product, category, changed_at, old_price, new_price,
                   round(((new_price / old_price - 1) * 100)::numeric, 1)::float8 AS change_pct
            FROM (
                SELECT p.name || ' (' || p.unit || ')' AS product, p.category,
                       pp.valid_from AS changed_at,
                       (lag(pp.price) OVER w)::float8 AS old_price, pp.price::float8 AS new_price
                FROM product_prices pp JOIN products p USING (product_id)
                WINDOW w AS (PARTITION BY pp.product_id ORDER BY pp.valid_from)
            ) c
            WHERE old_price IS NOT NULL AND changed_at >= now() - make_interval(days => %(days)s)
            ORDER BY changed_at DESC LIMIT %(limit)s
            """,
            {"days": days, "limit": limit},
        )


class GoldLake:
    TABLES = ("sales_daily", "store_daily", "sales_monthly")

    def __init__(self, lake_dir: Path) -> None:
        self.gold_dir = lake_dir / "gold"
        self._con = duckdb.connect()
        self._con.execute("INSTALL delta; LOAD delta")
        if self.available():
            for table in self.TABLES:
                path = str(self.gold_dir / table).replace("'", "''")
                # A view re-reads the table's latest Delta version on every query.
                self._con.execute(f"CREATE VIEW {table} AS SELECT * FROM delta_scan('{path}')")

    def available(self) -> bool:
        return all((self.gold_dir / t / "_delta_log").is_dir() for t in self.TABLES)

    def _frame(self, sql: str, params: list | None = None) -> pd.DataFrame:
        # One cursor per query: a DuckDB connection is not safe to share across the
        # threads Streamlit serves sessions on, but its cursors are.
        return self._con.cursor().execute(sql, params or []).df()

    def data_through(self) -> date | None:
        return self._con.cursor().execute("SELECT max(event_date) FROM store_daily").fetchone()[0]

    def monthly_by_store(self, months: int, store_ids: list[str]) -> pd.DataFrame:
        return self._frame(
            """
            SELECT month, store_id, sum(revenue)::double AS revenue
            FROM sales_monthly
            WHERE month >= date_trunc('month', current_date) - to_months(? - 1)
              AND list_contains(?, store_id)
            GROUP BY month, store_id ORDER BY month, store_id
            """,
            [months, store_ids],
        )

    def daily_by_store(self, days: int, store_ids: list[str]) -> pd.DataFrame:
        return self._frame(
            """
            SELECT event_date, store_id, invoices, revenue::double AS revenue,
                   avg_basket_value::double AS avg_basket_value
            FROM store_daily
            WHERE event_date > (SELECT max(event_date) FROM store_daily) - to_days(?)
              AND list_contains(?, store_id)
            ORDER BY event_date, store_id
            """,
            [days, store_ids],
        )

    def revenue_by_category(self, months: int, store_ids: list[str]) -> pd.DataFrame:
        return self._frame(
            """
            SELECT category, sum(units)::bigint AS units, sum(revenue)::double AS revenue
            FROM sales_monthly
            WHERE month >= date_trunc('month', current_date) - to_months(? - 1)
              AND list_contains(?, store_id)
            GROUP BY category ORDER BY revenue DESC
            """,
            [months, store_ids],
        )
