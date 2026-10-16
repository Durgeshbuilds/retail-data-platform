-- Tables written by the streaming job (phase 3). Idempotent, so it can also be
-- applied to an existing database: make db-migrate

-- Supplier deliveries. Doubles as the idempotency ledger for stock-receipts:
-- a receipt_id already here has already been added to stock.
CREATE TABLE IF NOT EXISTS stock_receipts (
    receipt_id   TEXT PRIMARY KEY,
    store_id     TEXT NOT NULL REFERENCES stores (store_id),
    product_id   TEXT NOT NULL REFERENCES products (product_id),
    qty          INTEGER NOT NULL CHECK (qty > 0),
    received_at  TIMESTAMPTZ NOT NULL,
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Running sales per store-day, updated in the same transaction as stock, so the
-- dashboard can show today's numbers without waiting for the batch lake build.
CREATE TABLE IF NOT EXISTS live_sales_daily (
    sale_date   DATE NOT NULL,           -- store-local (IST) date
    store_id    TEXT NOT NULL,
    product_id  TEXT NOT NULL,
    units       BIGINT NOT NULL,
    revenue     NUMERIC(14, 2) NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sale_date, store_id, product_id)
);
