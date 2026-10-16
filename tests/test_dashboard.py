from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest
import streamlit as st
from psycopg.conninfo import make_conninfo
from streamlit.testing.v1 import AppTest

from retail import config
from retail.dashboard.charts import inr
from retail.dashboard.data import GoldLake, LiveStore
from retail.streaming.live_store import PriceChange, SaleLine, apply_batch

DAY = date(2026, 10, 16)
ALL = ["BLR01", "HYD01", "PUN01"]


def test_inr_uses_indian_grouping():
    assert inr(0) == "₹0"
    assert inr(999) == "₹999"
    assert inr(123456) == "₹1,23,456"
    assert inr(220003208.4) == "₹22,00,03,208"
    assert inr(-1500) == "-₹1,500"


@pytest.fixture
def reader_dsn(pg_dsn):
    """Sales, a stock-out and a price change in a test database; returns the reader DSN."""
    with psycopg.connect(pg_dsn) as conn:
        stock = conn.execute("SELECT stock_qty FROM inventory_current"
                             " WHERE store_id = 'BLR01' AND product_id = 'P0035'").fetchone()[0]
        apply_batch(conn, [
            SaleLine("a", 1, "BLR01", "P0035", stock + 5, Decimal("40.00"), DAY),  # -> negative
            SaleLine("a", 2, "BLR01", "P0001", 2, Decimal("28.00"), DAY),
            SaleLine("b", 1, "PUN01", "P0001", 1, Decimal("28.00"), DAY),
        ], [], [PriceChange("P0002", Decimal("36.00"), datetime.now(UTC))])
        level = conn.execute("SELECT reorder_level FROM inventory_current"
                             " WHERE store_id = 'HYD01' AND product_id = 'P0002'").fetchone()[0]
        conn.execute("UPDATE inventory_current SET stock_qty = %s"
                     " WHERE store_id = 'HYD01' AND product_id = 'P0002'", (level - 1,))
    return make_conninfo(pg_dsn, user="retail_reader", password="retail_reader")


@pytest.mark.postgres
def test_live_queries(reader_dsn):
    live = LiveStore(reader_dsn)

    by_store = live.sales_by_store(DAY).set_index("store_id")
    assert by_store.loc["BLR01", "invoices"] == 1 and by_store.loc["PUN01", "invoices"] == 1
    assert by_store.loc["HYD01", "revenue"] == 0  # stores with no sales still appear

    top = live.top_products(DAY, ALL)
    assert top.iloc[0]["product"] == "Onion (1 kg)"
    assert live.top_products(DAY, ["HYD01"]).empty

    alerts = live.stock_alerts(ALL)
    assert list(alerts.status) == ["negative", "low"]  # most urgent first
    assert live.stock_alerts(["PUN01"]).empty

    changes = live.price_changes()
    assert list(changes["product"]) == ["Amul Gold Full Cream Milk (500 ml)"]
    assert changes.iloc[0]["change_pct"] == pytest.approx(5.9)


@pytest.mark.postgres
def test_reader_role_cannot_write(reader_dsn):
    with psycopg.connect(reader_dsn) as conn, pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        conn.execute("UPDATE inventory_current SET stock_qty = 0")


@pytest.mark.postgres
def test_app_renders_alerts(reader_dsn, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "POSTGRES_READER_DSN", reader_dsn)
    monkeypatch.setattr(config, "LAKE_DIR", tmp_path / "no-lake")
    st.cache_resource.clear()
    st.cache_data.clear()

    at = AppTest.from_file(str(config.PROJECT_ROOT / "src/retail/dashboard/app.py"),
                           default_timeout=30).run()

    assert not at.exception
    metrics = {m.label: m.value for m in at.metric}
    assert metrics["⚠️ Low stock items"] == "1" and metrics["⛔ Out or negative"] == "1"
    alerts = next(df.value for df in at.dataframe if "status" in df.value.columns)
    assert list(alerts["status"]) == ["⛔ Negative", "⚠️ Low"]
    assert any("make lake" in i.value for i in at.info)  # history tab without a lake
    st.cache_resource.clear()


def test_gold_lake_reports_missing_tables(tmp_path):
    assert not GoldLake(tmp_path).available()


@pytest.mark.spark
def test_gold_queries(spark, tmp_path):
    from retail.catalog import load_products, load_stores, store_today
    from retail.lake import bronze, dims, gold, silver
    from retail.lake.tables import Lake
    from retail.simulator.history import generate

    lake, raw = Lake(tmp_path / "lake"), tmp_path / "raw"
    today = store_today()
    generate(load_products(), load_stores(), today.replace(day=1), today, 20, raw, seed=3)
    if not any(raw.rglob("*.gz")):
        pytest.skip("first day of the month: no history yet this month")
    dims.build(spark, lake)
    bronze.ingest_files(spark, lake, raw)
    silver.build(spark, lake)
    gold.build(spark, lake)

    g = GoldLake(lake.root)
    assert g.available()
    monthly = g.monthly_by_store(3, ALL)
    assert set(monthly.store_id) == set(ALL)
    assert g.revenue_by_category(3, ["BLR01"]).revenue.sum() == pytest.approx(
        monthly[monthly.store_id == "BLR01"].revenue.sum())
    assert g.daily_by_store(90, ALL).event_date.max().date() == g.data_through()
