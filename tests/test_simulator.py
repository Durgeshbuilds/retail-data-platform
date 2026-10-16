import gzip
import random
from datetime import UTC, date, datetime

from retail.catalog import Store, load_products, load_stores, price_in_month
from retail.events import Invoice
from retail.simulator.history import generate
from retail.simulator.invoices import InvoiceFactory

PRODUCTS = load_products()
PRICES = {p.product_id: p.price for p in PRODUCTS}
TS = datetime(2026, 10, 16, 9, 30, tzinfo=UTC)


def build_many(seed: int, n: int = 50) -> list[Invoice]:
    factory = InvoiceFactory(PRODUCTS, random.Random(seed))
    return [factory.build("BLR01", TS, PRICES) for _ in range(n)]


def test_factory_is_deterministic():
    assert build_many(7) == build_many(7)
    assert build_many(7) != build_many(8)


def test_factory_uses_given_prices():
    for invoice in build_many(1):
        for line in invoice.lines:
            assert line.unit_price == PRICES[line.product_id]


def test_popular_products_sell_more():
    counts: dict[str, int] = {}
    for invoice in build_many(3, n=2000):
        for line in invoice.lines:
            counts[line.product_id] = counts.get(line.product_id, 0) + 1
    assert counts["P0035"] > 5 * counts.get("P0063", 0)  # onions (10) vs mutton (2)


def read_invoices(root) -> list[Invoice]:
    invoices = []
    for path in sorted(root.rglob("*.jsonl.gz")):
        with gzip.open(path, "rt") as f:
            invoices += [Invoice.model_validate_json(line) for line in f]
    return invoices


def test_history_files(tmp_path):
    stores = load_stores() + [Store("NEW01", "New", "Chennai", date(2026, 9, 30), 1.0)]
    written = generate(PRODUCTS, stores, date(2026, 9, 29), date(2026, 10, 2),
                       invoices_per_day=20, out_dir=tmp_path, seed=1)

    files = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*"))
    assert "2026-09/BLR01-2026-09-29.jsonl.gz" in files
    assert "2026-10/HYD01-2026-10-01.jsonl.gz" in files
    assert len([f for f in files if f.endswith(".gz")]) == 3 * 3 + 2  # NEW01 opened 30 Sep
    assert not [f for f in files if f.endswith(".tmp")]

    invoices = read_invoices(tmp_path)
    assert len(invoices) == written

    # September invoices carry September prices, not today's.
    sept = next(i for i in invoices if i.event_time.month == 9 and i.store_id == "BLR01")
    line = sept.lines[0]
    assert line.unit_price == price_in_month(PRICES[line.product_id], date(2026, 9, 1))


def test_history_rerun_only_adds_new_days(tmp_path):
    stores = load_stores()
    first = generate(PRODUCTS, stores, date(2026, 9, 29), date(2026, 10, 1), 20, tmp_path, 1)
    before = {p: p.read_bytes() for p in tmp_path.rglob("*.gz")}

    # A later run covering more days writes only the new ones...
    added = generate(PRODUCTS, stores, date(2026, 9, 28), date(2026, 10, 2), 20, tmp_path, 1)
    assert added > 0
    assert all(p.read_bytes() == content for p, content in before.items())

    # ...and a store-day's content doesn't depend on the range it was generated in.
    fresh = tmp_path / "fresh"
    generate(PRODUCTS, stores, date(2026, 9, 29), date(2026, 9, 30), 20, fresh, 1)
    name = "2026-09/BLR01-2026-09-29.jsonl.gz"
    with gzip.open(fresh / name, "rt") as a, gzip.open(tmp_path / name, "rt") as b:
        assert a.read() == b.read()
    assert first > 0
