from datetime import date
from decimal import Decimal

from retail.catalog import add_months, historical_price, load_products, load_stores
from retail.seed import price_history


def test_product_master_is_clean():
    products = load_products()
    ids = [p.product_id for p in products]
    assert len(products) == 200
    assert len(set(ids)) == len(ids)
    assert all(1 <= p.popularity <= 10 for p in products)
    assert all(p.price > 0 for p in products)
    assert len({(p.name, p.unit) for p in products}) == len(products)


def test_stores_load():
    stores = load_stores()
    assert {s.store_id for s in stores} == {"BLR01", "PUN01", "HYD01"}


def test_add_months_crosses_years():
    assert add_months(date(2026, 1, 1), -1) == date(2025, 12, 1)
    assert add_months(date(2025, 11, 1), 3) == date(2026, 2, 1)


def test_historical_price():
    today = date(2026, 10, 16)
    assert historical_price(Decimal("28.00"), date(2026, 10, 1), today) == Decimal("28.00")
    year_ago = historical_price(Decimal("650.00"), date(2025, 10, 5), today)
    assert year_ago == Decimal("612.00")  # 650 / 1.005^12, whole rupees


def test_price_history_is_contiguous_and_ends_open():
    product = next(p for p in load_products() if p.product_id == "P0013")  # Amul Ghee 650
    rows = price_history(product, date(2026, 10, 16), months=12)
    assert rows[-1][1] is None and rows[-1][2] == Decimal("650.00")
    for older, newer in zip(rows, rows[1:], strict=False):
        assert older[1] == newer[0]
        assert older[2] < newer[2]
