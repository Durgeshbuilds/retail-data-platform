"""Search, query engine and API against a throwaway database and lake.

Uses the real embedding model (downloaded once into data/models).
"""

from datetime import date, timedelta
from decimal import Decimal

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.conninfo import make_conninfo

from retail.catalog import load_products, load_stores, store_today
from retail.lake import bronze, dims, gold, silver
from retail.lake.tables import Lake
from retail.qa.engine import QueryEngine
from retail.search.embedder import Embedder
from retail.search.index import index_products
from retail.search.resolve import ProductResolver
from retail.simulator.history import generate
from retail.streaming.live_store import SaleLine, apply_batch

pytestmark = [pytest.mark.postgres, pytest.mark.model]

MILKS = {"Amul Gold Full Cream Milk", "Amul Kool Kesar Flavoured Milk", "Amul Taaza Toned Milk",
         "Mother Dairy Toned Milk", "Nandini Toned Milk", "Nestle a+ Slim Skimmed Milk",
         "Sofit Soya Milk Plain"}


@pytest.fixture(scope="module")
def embedder():
    return Embedder()


@pytest.fixture
def indexed(pg_dsn, embedder):
    with psycopg.connect(pg_dsn) as conn:
        assert index_products(conn, embedder) == 200
        assert index_products(conn, embedder) == 0  # nothing changed, nothing re-embedded
        conn.execute("UPDATE products SET name = 'Amul Taaza Toned Milk Pouch'"
                     " WHERE product_id = 'P0001'")
        assert index_products(conn, embedder) == 1
        conn.execute("UPDATE products SET name = 'Amul Taaza Toned Milk'"
                     " WHERE product_id = 'P0001'")
        index_products(conn, embedder)
    return make_conninfo(pg_dsn, user="retail_reader", password="retail_reader")


@pytest.mark.parametrize(("query", "match_type", "selected", "excluded"), [
    ("milks", "name", MILKS,
     {"Amul Milk Chocolate", "Cadbury Dairy Milk Chocolate", "Cadbury Dairy Milk Silk"}),
    ("rice", "name", {"Daawat Rozana Basmati Rice", "India Gate Classic Basmati Rice",
                      "Lalitha Sona Masoori Rice"}, set()),
    ("chocolate", "name", {"Amul Milk Chocolate", "Cadbury Dairy Milk Chocolate"},
     {"Kwality Wall's Cornetto Double Chocolate"}),
])
def test_hybrid_resolution(indexed, embedder, query, match_type, selected, excluded):
    r = ProductResolver(indexed, embedder).resolve(query)
    assert r.match_type == match_type
    assert {p.name for p in r.selected} == selected
    assert {p.name for p in r.excluded} == excluded


def test_category_brand_and_semantic(indexed, embedder):
    resolver = ProductResolver(indexed, embedder)
    dairy = resolver.resolve("dairy")
    assert dairy.match_type == "category" and {p.category for p in dairy.selected} == {"Dairy"}
    amul = resolver.resolve("AMUL")
    assert amul.match_type == "brand" and len({p.category for p in amul.selected}) > 1
    fuzzy = resolver.resolve("fizzy cola")
    assert fuzzy.match_type == "semantic" and 0 < len(fuzzy.selected) <= 8
    assert "Check them" in fuzzy.note


@pytest.fixture
def lake_and_live(spark, pg_dsn, tmp_path):
    """Gold built from two days of history; the live store has a later day of sales."""
    lake, raw = Lake(tmp_path / "lake"), tmp_path / "raw"
    generate(load_products(), load_stores(), date(2026, 9, 29), date(2026, 10, 1), 60, raw, 7)
    dims.build(spark, lake)
    bronze.ingest_files(spark, lake, raw)
    silver.build(spark, lake)
    gold.build(spark, lake)
    with psycopg.connect(pg_dsn) as conn:
        apply_batch(conn, [
            SaleLine("x", 1, "BLR01", "P0001", 4, Decimal("28.00"), store_today()),
            # A day before the cut-over: gold is authoritative, so this must be ignored.
            SaleLine("y", 1, "BLR01", "P0001", 1000, Decimal("28.00"), date(2026, 9, 29)),
        ], [], [])
    reader = make_conninfo(pg_dsn, user="retail_reader", password="retail_reader")
    return lake, reader


@pytest.mark.spark
def test_engine_splits_gold_and_live_at_cutover(spark, lake_and_live):
    lake, reader = lake_and_live
    engine = QueryEngine(lake.root, reader)
    assert engine.cutover() == store_today()  # gold was built today

    gold_units = (spark.read.format("delta").load(lake.gold_sales_daily)
                  .filter("product_id = 'P0001' AND store_id = 'BLR01'")
                  .groupBy().sum("units").first()[0]) or 0
    assert gold_units > 0  # so the test really mixes gold and live rows
    result = engine.run("sales", start=date(2026, 1, 1), end=store_today(), store_ids=["BLR01"],
                        product_ids=["P0001"])
    assert result.rows[0][1] == gold_units + 4  # gold history + today's live, no overlap

    stock = engine.run("stock", store_ids=["BLR01"], product_ids=["P0001"])
    assert stock.columns == ["store_id", "product", "stock_qty", "reorder_level"]


def test_engine_without_lake_reads_live_only(pg_dsn, tmp_path):
    reader = make_conninfo(pg_dsn, user="retail_reader", password="retail_reader")
    with psycopg.connect(pg_dsn) as conn:
        apply_batch(conn, [SaleLine("z", 1, "PUN01", "P0002", 3, Decimal("34.00"),
                                    store_today() - timedelta(days=2))], [], [])
    engine = QueryEngine(tmp_path / "no-lake", reader)
    assert engine.cutover() == date.min
    rows = engine.run("sales", start=store_today() - timedelta(days=7), end=store_today(),
                      store_ids=["PUN01"], product_ids=None).rows
    assert rows == [["Amul Gold Full Cream Milk (500 ml)", 3, 102.0]] or rows == [
        ("Amul Gold Full Cream Milk (500 ml)", 3, 102.0)]


@pytest.mark.spark
def test_api_ask_and_override(lake_and_live, indexed):
    from retail.api.main import app, build_service

    lake, reader = lake_and_live
    app.state.qa = build_service(reader, lake.root)
    try:
        with TestClient(app) as client:
            body = client.post("/ask", json={"question": "how many milks sold this year"}).json()
            assert body["intent"] == "sales" and body["period"]["label"].endswith("to date")
            assert {p["name"] for p in body["match"]["selected"]} == MILKS
            assert "SELECT" in body["sql"] and body["params"]["products"]

            only = client.post("/ask", json={"question": "how many milks sold this year",
                                             "product_ids": ["P0001"]}).json()
            assert [p["product_id"] for p in only["match"]["selected"]] == ["P0001"]
            assert len(only["match"]["excluded"]) == 9

            assert client.get("/search", params={"q": "rice"}).json()["match_type"] == "name"
            assert client.post("/ask", json={"question": "x"}).status_code == 422
            no_product = client.post("/ask", json={"question": "stock please"}).json()
            assert no_product["sql"] is None and "Which product" in no_product["answer"]
    finally:
        del app.state.qa
