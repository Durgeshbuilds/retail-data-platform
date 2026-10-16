-- Phase 4: dashboard support. Idempotent (make db-migrate).

-- Invoices per store-day, maintained by the streaming job in the same statement
-- as stock and sales. An invoice's lines always arrive in one Kafka message, so
-- they are claimed together and counting distinct invoice_ids per batch is exact.
CREATE TABLE IF NOT EXISTS live_store_daily (
    sale_date   DATE NOT NULL,           -- store-local (IST) date
    store_id    TEXT NOT NULL,
    invoices    BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sale_date, store_id)
);

-- Read-only role for the dashboard and, later, the Q&A layer. Even a buggy or
-- LLM-written query cannot change data or run for long.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'retail_reader') THEN
        CREATE ROLE retail_reader LOGIN PASSWORD 'retail_reader';
    END IF;
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO retail_reader', current_database());
END
$$;
ALTER ROLE retail_reader SET default_transaction_read_only = on;
ALTER ROLE retail_reader SET statement_timeout = '5s';
GRANT USAGE ON SCHEMA public TO retail_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO retail_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO retail_reader;
