import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from retail.events import Invoice, PriceUpdate

TS = datetime(2026, 10, 16, 9, 30, 0, 123456, tzinfo=UTC)


def make_invoice(**overrides):
    fields = dict(
        invoice_id="inv-1",
        store_id="BLR01",
        event_time=TS,
        payment_method="upi",
        lines=[
            {"line_no": 1, "product_id": "P0001", "qty": 2, "unit_price": "28.00"},
            {"line_no": 2, "product_id": "P0021", "qty": 1, "unit_price": "45.50"},
        ],
        total_amount="101.50",
    )
    fields.update(overrides)
    return Invoice(**fields)


def test_invoice_json_shape():
    payload = json.loads(make_invoice().model_dump_json())
    assert payload["schema_version"] == 1
    assert payload["event_time"] == "2026-10-16T09:30:00.123+00:00"
    assert payload["total_amount"] == 101.5  # a JSON number, not a string
    assert payload["lines"][1]["unit_price"] == 45.5


def test_invoice_round_trips():
    invoice = make_invoice()
    again = Invoice.model_validate_json(invoice.model_dump_json())
    assert again.total_amount == Decimal("101.50")
    assert again.lines == invoice.lines


def test_total_must_match_lines():
    with pytest.raises(ValidationError, match="total_amount"):
        make_invoice(total_amount="100.00")


def test_line_numbers_must_be_sequential():
    lines = [
        {"line_no": 1, "product_id": "P0001", "qty": 1, "unit_price": "28.00"},
        {"line_no": 3, "product_id": "P0002", "qty": 1, "unit_price": "34.00"},
    ]
    with pytest.raises(ValidationError, match="line_no"):
        make_invoice(lines=lines, total_amount="62.00")


def test_duplicate_product_rejected():
    lines = [
        {"line_no": 1, "product_id": "P0001", "qty": 1, "unit_price": "28.00"},
        {"line_no": 2, "product_id": "P0001", "qty": 1, "unit_price": "28.00"},
    ]
    with pytest.raises(ValidationError, match="only one line"):
        make_invoice(lines=lines, total_amount="56.00")


def test_naive_timestamp_rejected():
    with pytest.raises(ValidationError):
        make_invoice(event_time=datetime(2026, 10, 16, 9, 30))


def test_unknown_field_rejected():
    with pytest.raises(ValidationError):
        make_invoice(customer_name="someone")


def test_price_update_must_change_price():
    with pytest.raises(ValidationError, match="differ"):
        PriceUpdate(product_id="P0001", old_price="28.00", new_price="28.00", effective_at=TS)
