"""Read-only query templates over gold history + live Postgres, in one DuckDB query.

Sales come from two places that must not overlap:

* gold.sales_daily          complete for days before the gold tables were built
* pg live_sales_daily       every day the streaming job has seen, kept current

The cut-over is the store-local date of the last gold build: earlier days come
from gold, that day onwards from the live store. DuckDB reads the Delta tables
directly and attaches Postgres read-only as retail_reader, so one SQL statement
(shown to the user) produces each answer.
"""

import json
import threading
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from retail.catalog import STORE_TZ

SALES = """
WITH sales AS (
    SELECT event_date AS day, store_id, product_id, units, revenue
    FROM gold_sales_daily WHERE event_date < $cutover
    UNION ALL
    SELECT sale_date, store_id, product_id, units, revenue
    FROM pg.public.live_sales_daily WHERE sale_date >= $cutover
)
"""

TEMPLATES = {
    "sales": SALES + """
SELECT p.name || ' (' || p.unit || ')' AS product,
       sum(s.units)::BIGINT AS units, sum(s.revenue)::DOUBLE AS revenue
FROM sales s JOIN pg.public.products p USING (product_id)
WHERE s.day BETWEEN $start AND $end
  AND list_contains($stores, s.store_id)
  AND ($all_products OR list_contains($products, s.product_id))
GROUP BY ALL ORDER BY {metric} DESC""",

    "top_products": SALES + """
SELECT p.name || ' (' || p.unit || ')' AS product, p.category,
       sum(s.units)::BIGINT AS units, sum(s.revenue)::DOUBLE AS revenue
FROM sales s JOIN pg.public.products p USING (product_id)
WHERE s.day BETWEEN $start AND $end
  AND list_contains($stores, s.store_id)
  AND ($all_products OR list_contains($products, s.product_id))
GROUP BY ALL ORDER BY {metric} DESC LIMIT $limit""",

    "top_categories": SALES + """
SELECT p.category, sum(s.units)::BIGINT AS units, sum(s.revenue)::DOUBLE AS revenue
FROM sales s JOIN pg.public.products p USING (product_id)
WHERE s.day BETWEEN $start AND $end
  AND list_contains($stores, s.store_id)
  AND ($all_products OR list_contains($products, s.product_id))
GROUP BY ALL ORDER BY {metric} DESC LIMIT $limit""",

    "stock": """
SELECT i.store_id, p.name || ' (' || p.unit || ')' AS product,
       i.stock_qty, i.reorder_level
FROM pg.public.inventory_current i JOIN pg.public.products p USING (product_id)
WHERE list_contains($stores, i.store_id)
  AND ($all_products OR list_contains($products, i.product_id))
ORDER BY product, i.store_id""",

    "price": """
SELECT p.name || ' (' || p.unit || ')' AS product, p.current_price::DOUBLE AS price
FROM pg.public.products p
WHERE $all_products OR list_contains($products, p.product_id)
ORDER BY product""",
}

# Parameters each template actually uses (DuckDB rejects unused ones).
PARAMS = {
    "sales": ["cutover", "start", "end", "stores", "all_products", "products"],
    "top_products": ["cutover", "start", "end", "stores", "all_products", "products", "limit"],
    "top_categories": ["cutover", "start", "end", "stores", "all_products", "products",
                       "limit"],
    "stock": ["stores", "all_products", "products"],
    "price": ["all_products", "products"],
}


@dataclass(frozen=True)
class QueryResult:
    sql: str
    params: dict
    columns: list[str]
    rows: list[tuple]


def gold_built_at(table_dir: Path) -> datetime | None:
    """Commit time of the table's latest Delta version, read from its transaction log."""
    commits = sorted((table_dir / "_delta_log").glob("*.json"))
    if not commits:
        return None
    for line in commits[-1].read_text().splitlines():
        info = json.loads(line).get("commitInfo")
        if info:
            return datetime.fromtimestamp(info["timestamp"] / 1000, UTC)
    return None


class QueryEngine:
    def __init__(self, lake_dir: Path, reader_dsn: str) -> None:
        self.gold_dir = lake_dir / "gold" / "sales_daily"
        self._con = duckdb.connect()
        self._con.execute("INSTALL delta; LOAD delta; INSTALL postgres; LOAD postgres")
        # ATTACH takes no bound parameters; the DSN comes from config, not from users.
        dsn = reader_dsn.replace("'", "''")
        self._con.execute(f"ATTACH '{dsn}' AS pg (TYPE postgres, READ_ONLY)")
        self._has_gold: bool | None = None
        self._lock = threading.Lock()
        self._sync_gold_view()

    def _sync_gold_view(self) -> None:
        """Point gold_sales_daily at the Delta table, or at an empty stand-in if there is
        none yet. Re-checked before every query: the lake may be built after start-up,
        and the cut-over date must always match what the view actually reads."""
        has_gold = gold_built_at(self.gold_dir) is not None
        if has_gold == self._has_gold:
            return
        with self._lock:
            if has_gold:
                path = str(self.gold_dir).replace("'", "''")
                self._con.execute("CREATE OR REPLACE VIEW gold_sales_daily AS "
                                  f"SELECT * FROM delta_scan('{path}')")
            else:  # no lake yet: every day comes from the live store
                self._con.execute(
                    "CREATE OR REPLACE VIEW gold_sales_daily AS SELECT DATE '1970-01-01'"
                    " AS event_date, '' AS store_id, '' AS product_id, 0 AS units,"
                    " 0::DOUBLE AS revenue LIMIT 0")
            self._has_gold = has_gold

    def cutover(self) -> date:
        """First day served from the live store rather than gold."""
        built = gold_built_at(self.gold_dir)
        return built.astimezone(STORE_TZ).date() if built else date.min

    def store_ids(self) -> list[str]:
        return [r[0] for r in self._con.cursor().execute(
            "SELECT store_id FROM pg.public.stores ORDER BY 1").fetchall()]

    def run(self, intent: str, metric: str = "units", *, start: date | None = None,
            end: date | None = None, store_ids: list[str], product_ids: list[str] | None,
            limit: int = 5) -> QueryResult:
        if metric not in ("units", "revenue"):
            raise ValueError(f"unknown metric {metric!r}")
        self._sync_gold_view()
        sql = TEMPLATES[intent].format(metric=metric).strip()
        values = {
            "cutover": self.cutover(), "start": start, "end": end, "stores": store_ids,
            "all_products": product_ids is None, "products": product_ids or [], "limit": limit,
        }
        params = {k: values[k] for k in PARAMS[intent]}
        cursor = self._con.cursor()  # one cursor per query: safe across API threads
        cursor.execute(sql, params)
        columns = [d[0] for d in cursor.description]
        return QueryResult(sql, params, columns, cursor.fetchall())
