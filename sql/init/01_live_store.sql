-- Live operational store: current stock and prices, updated by the
-- streaming job. History and analytics live in Delta Lake, not here.

-- Timestamps are stored as UTC (timestamptz); sessions display them in store time.
ALTER DATABASE retail SET timezone TO 'Asia/Kolkata';

CREATE TABLE stores (
    store_id    TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    city        TEXT NOT NULL,
    opened_on   DATE NOT NULL
);

CREATE TABLE products (
    product_id     TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    category       TEXT NOT NULL,
    brand          TEXT NOT NULL,
    unit           TEXT NOT NULL,              -- e.g. "1 L", "500 g"
    current_price  NUMERIC(10, 2) NOT NULL CHECK (current_price > 0)
);

-- Price history (SCD type 2). The open row has valid_to = NULL.
CREATE TABLE product_prices (
    product_id  TEXT NOT NULL REFERENCES products (product_id),
    price       NUMERIC(10, 2) NOT NULL CHECK (price > 0),
    valid_from  TIMESTAMPTZ NOT NULL,
    valid_to    TIMESTAMPTZ,
    PRIMARY KEY (product_id, valid_from)
);

-- At most one open price per product.
CREATE UNIQUE INDEX product_prices_one_open
    ON product_prices (product_id) WHERE valid_to IS NULL;

-- Stock is per store. stock_qty may go negative: that means sales were
-- recorded faster than stock arrived, and is surfaced as an alert
-- rather than hidden by clamping to zero.
CREATE TABLE inventory_current (
    store_id       TEXT NOT NULL REFERENCES stores (store_id),
    product_id     TEXT NOT NULL REFERENCES products (product_id),
    stock_qty      INTEGER NOT NULL,
    reorder_level  INTEGER NOT NULL CHECK (reorder_level >= 0),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (store_id, product_id)
);

-- Idempotency ledger for the streaming job. Spark can replay a
-- micro-batch; a line already recorded here must not decrement stock again.
CREATE TABLE processed_lines (
    invoice_id    TEXT NOT NULL,
    line_no       SMALLINT NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (invoice_id, line_no)
);
