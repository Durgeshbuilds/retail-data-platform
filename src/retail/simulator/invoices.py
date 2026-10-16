"""Builds realistic invoices: popular products sell more and basket sizes vary."""

import itertools
import math
import random
import uuid
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal

from retail.catalog import Product
from retail.events import Invoice, InvoiceLine

MEAN_EXTRA_LINES = 3.0  # baskets average ~4 distinct products
MAX_LINES = 15
QTY_CHOICES = [1, 2, 3, 4]
QTY_WEIGHTS = [70, 20, 8, 2]
EXPENSIVE = Decimal("500")  # nobody buys three packs of nappies at once
PAYMENT_METHODS = ["upi", "card", "cash"]
PAYMENT_WEIGHTS = [55, 25, 20]


class InvoiceFactory:
    def __init__(self, products: list[Product], rng: random.Random) -> None:
        self._products = products
        self._rng = rng
        self._cum_weights = list(itertools.accumulate(p.popularity for p in products))

    def build(
        self, store_id: str, event_time: datetime, prices: Mapping[str, Decimal]
    ) -> Invoice:
        """One invoice for `store_id`, priced from `prices` (product_id -> unit price)."""
        lines = [
            InvoiceLine(
                line_no=line_no,
                product_id=product.product_id,
                qty=self._quantity(prices[product.product_id]),
                unit_price=prices[product.product_id],
            )
            for line_no, product in enumerate(self._basket(), start=1)
        ]
        return Invoice(
            invoice_id=str(uuid.UUID(int=self._rng.getrandbits(128), version=4)),
            store_id=store_id,
            event_time=event_time,
            payment_method=self._rng.choices(PAYMENT_METHODS, PAYMENT_WEIGHTS)[0],
            lines=lines,
            total_amount=sum((line.qty * line.unit_price for line in lines), Decimal("0")),
        )

    def _basket(self) -> list[Product]:
        size = min(1 + self._poisson(MEAN_EXTRA_LINES), MAX_LINES)
        chosen: dict[str, Product] = {}
        while len(chosen) < size:
            product = self._rng.choices(self._products, cum_weights=self._cum_weights)[0]
            chosen.setdefault(product.product_id, product)
        return list(chosen.values())

    def _quantity(self, price: Decimal) -> int:
        if price >= EXPENSIVE:
            return 1
        return self._rng.choices(QTY_CHOICES, QTY_WEIGHTS)[0]

    def _poisson(self, mean: float) -> int:
        # Knuth's method; fine for small means.
        limit, k, p = math.exp(-mean), 0, 1.0
        while True:
            p *= self._rng.random()
            if p <= limit:
                return k
            k += 1
