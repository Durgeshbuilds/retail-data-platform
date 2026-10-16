"""Event contracts for the Kafka topics.

These models are the single source of truth for the event shape. The live
simulator and the history generator both serialise through them, so the bronze
layer receives identical JSON from the streaming and batch paths.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, PlainSerializer, model_validator

# Money is exact (Decimal) in Python and a plain JSON number on the wire, so Spark
# can read it straight into DECIMAL(10, 2). Values never have more than 2 decimals,
# so float() reproduces them exactly as text.
Money = Annotated[
    Decimal,
    Field(gt=0, max_digits=10, decimal_places=2),
    PlainSerializer(float, return_type=float, when_used="json"),
]

# Always UTC with millisecond precision, e.g. 2026-10-16T09:12:03.123+00:00.
Timestamp = Annotated[
    AwareDatetime,
    PlainSerializer(
        lambda dt: dt.astimezone(UTC).isoformat(timespec="milliseconds"),
        return_type=str,
        when_used="json",
    ),
]


class _Model(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Event(_Model):
    schema_version: Literal[1] = 1


class InvoiceLine(_Model):
    line_no: int = Field(ge=1)
    product_id: str
    qty: int = Field(gt=0)
    unit_price: Money  # price at the moment of sale, so revenue survives later price changes


class Invoice(Event):
    """A completed checkout. Topic: invoices, key: invoice_id."""

    invoice_id: str
    store_id: str
    event_time: Timestamp
    payment_method: Literal["upi", "card", "cash"]
    lines: list[InvoiceLine] = Field(min_length=1)
    total_amount: Money

    @model_validator(mode="after")
    def _check_lines(self) -> "Invoice":
        if [line.line_no for line in self.lines] != list(range(1, len(self.lines) + 1)):
            raise ValueError("line_no must run 1..n in order")
        if len({line.product_id for line in self.lines}) != len(self.lines):
            raise ValueError("each product may appear on only one line")
        expected = sum((line.qty * line.unit_price for line in self.lines), Decimal("0"))
        if self.total_amount != expected:
            raise ValueError(f"total_amount {self.total_amount} != sum of lines {expected}")
        return self


class PriceUpdate(Event):
    """A list-price change. Topic: price-updates, key: product_id."""

    product_id: str
    old_price: Money
    new_price: Money
    effective_at: Timestamp

    @model_validator(mode="after")
    def _check_changed(self) -> "PriceUpdate":
        if self.new_price == self.old_price:
            raise ValueError("new_price must differ from old_price")
        return self


class StockReceipt(Event):
    """A supplier delivery into one store. Topic: stock-receipts, key: store_id."""

    receipt_id: str
    store_id: str
    product_id: str
    qty: int = Field(gt=0)
    received_at: Timestamp


def utc_now() -> datetime:
    return datetime.now(UTC)
