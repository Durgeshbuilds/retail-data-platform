import gzip
import random
from datetime import UTC, date, datetime

from retail.catalog import Store, historical_price, load_products, load_stores
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


def test_history_files(tmp_path):
    stores = load_stores() + [Store("NEW01", "New", "Chennai", date(2026, 9, 30), 1.0)]
    written = generate(PRODUCTS, stores, date(2026, 9, 29), date(2026, 10, 2),
                       invoices_per_day=20, out_dir=tmp_path, seed=1)

    files = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*.gz"))
    assert files == [
        "2026-09/BLR01.jsonl.gz", "2026-09/HYD01.jsonl.gz",
        "2026-09/NEW01.jsonl.gz", "2026-09/PUN01.jsonl.gz",
        "2026-10/BLR01.jsonl.gz", "2026-10/HYD01.jsonl.gz",
        "2026-10/NEW01.jsonl.gz", "2026-10/PUN01.jsonl.gz",
    ]

    invoices = []
    for path in tmp_path.rglob("*.gz"):
        with gzip.open(path, "rt") as f:
            invoices += [Invoice.model_validate_json(line) for line in f]
    assert len(invoices) == written

    new_store = [i for i in invoices if i.store_id == "NEW01"]
    assert new_store and min(i.event_time for i in new_store).date() >= date(2026, 9, 29)

    # September invoices carry September prices, not today's.
    sept = next(i for i in invoices if i.event_time.month == 9 and i.store_id == "BLR01")
    line = sept.lines[0]
    expected = historical_price(PRICES[line.product_id], date(2026, 9, 1), date(2026, 10, 2))
    assert line.unit_price == expected
