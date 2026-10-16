"""Reference data (products, stores) and the business rules derived from it.

Both the seed script and the simulators import from here, so opening stock,
reorder levels and historical prices always agree with each other.
"""

import csv
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from retail.config import SEEDS_DIR

STORE_TZ = ZoneInfo("Asia/Kolkata")

# How many months of history the seed (price history) and history generator cover.
DEFAULT_HISTORY_MONTHS = 12

# Prices drift up ~0.5% a month, so a year ago things were ~6% cheaper.
MONTHLY_INFLATION = Decimal("0.005")

# Reorder level = popularity x this x store size. Restocks refill to reorder level x
# TARGET_STOCK_MULTIPLE, and opening stock starts at that same target.
REORDER_UNITS_PER_POPULARITY = 10
TARGET_STOCK_MULTIPLE = 4


@dataclass(frozen=True)
class Product:
    product_id: str
    name: str
    category: str
    brand: str
    unit: str
    price: Decimal  # current list price
    popularity: int  # 1 (rarely bought) to 10 (in most baskets)


@dataclass(frozen=True)
class Store:
    store_id: str
    name: str
    city: str
    opened_on: date
    size_factor: float  # relative footfall; 1.0 is an average store


def load_products(path: Path = SEEDS_DIR / "products.csv") -> list[Product]:
    with path.open(newline="") as f:
        return [
            Product(
                product_id=row["product_id"],
                name=row["name"],
                category=row["category"],
                brand=row["brand"],
                unit=row["unit"],
                price=Decimal(row["price"]),
                popularity=int(row["popularity"]),
            )
            for row in csv.DictReader(f)
        ]


def load_stores(path: Path = SEEDS_DIR / "stores.csv") -> list[Store]:
    with path.open(newline="") as f:
        return [
            Store(
                store_id=row["store_id"],
                name=row["name"],
                city=row["city"],
                opened_on=date.fromisoformat(row["opened_on"]),
                size_factor=float(row["size_factor"]),
            )
            for row in csv.DictReader(f)
        ]


def reorder_level(product: Product, store: Store) -> int:
    return max(5, round(product.popularity * REORDER_UNITS_PER_POPULARITY * store.size_factor))


def add_months(month_start: date, n: int) -> date:
    """Shift the first day of a month by n months (n may be negative)."""
    years, month_index = divmod(month_start.month - 1 + n, 12)
    return date(month_start.year + years, month_index + 1, 1)


def months_between(earlier: date, later: date) -> int:
    return (later.year - earlier.year) * 12 + later.month - earlier.month


def historical_price(current_price: Decimal, day: date, today: date) -> Decimal:
    """List price in effect during the calendar month containing `day`.

    The current month uses the current price; earlier months are deflated by
    MONTHLY_INFLATION per month and rounded to whole rupees.
    """
    months_ago = months_between(day, today)
    if months_ago <= 0:
        return current_price
    price = current_price / (1 + MONTHLY_INFLATION) ** months_ago
    rupees = max(price.quantize(Decimal("1"), ROUND_HALF_UP), Decimal("1"))
    return rupees.quantize(Decimal("0.01"))
