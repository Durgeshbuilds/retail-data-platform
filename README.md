# Retail Data Platform

A supermarket data platform: invoices stream through Kafka and Spark into
live per-store stock in Postgres, while batch jobs build a Delta Lake for
history and analytics.

> Work in progress — built phase by phase. Status: **Phase 1 (foundation) complete.**

## Prerequisites (macOS, Apple Silicon)

| Tool | Version | Install |
|---|---|---|
| Docker Desktop | latest | https://www.docker.com/products/docker-desktop/ — cap memory at ~4 GB |
| Java | 17 | `brew install openjdk@17` |
| Python | 3.12 | `brew install uv` then `uv python install 3.12` |

Spark runs natively (not in Docker) to save memory.

## Quick start

```bash
make up          # Kafka (KRaft) + Postgres, waits until healthy
make topics      # invoices, price-updates, stock-receipts
make install     # .venv on Python 3.12
make seed        # 3 stores, 200 products, 12 months of price history, opening stock
make history     # ~370k historical invoices -> data/raw/invoices/YYYY-MM/STORE.jsonl.gz
make simulate    # live invoices, price changes and restocks to Kafka (Ctrl-C to stop)
make peek TOPIC=invoices
make test
```

`make down` stops everything; `make reset` also deletes the data.

## Data

**Reference data** lives in [`seeds/`](seeds): 200 products across 12 categories
(Indian brands, INR prices, a 1–10 popularity weight) and 3 stores with a size
factor that scales their footfall.

**Events** are defined once, as Pydantic models in
[`src/retail/events.py`](src/retail/events.py), and validated on the way out:

| Topic | Key | Event | Notes |
|---|---|---|---|
| `invoices` | invoice_id | `Invoice` | Lines carry the unit price at time of sale; total must equal the lines |
| `price-updates` | product_id | `PriceUpdate` | Keyed by product so changes to one product stay in order |
| `stock-receipts` | store_id | `StockReceipt` | Supplier deliveries, triggered at the reorder level |

Money is exact `Decimal` in Python and a JSON number on the wire; timestamps are
UTC ISO-8601 with milliseconds.

**Historical files** use the exact same `Invoice` JSON as the Kafka topic, so the
bronze layer ingests the batch and streaming paths with one schema. Historical
prices are deflated ~0.5% a month, and the seeded `product_prices` history
matches them month for month. Output is deterministic for a given `--seed`.

**Live simulator** — invoices arrive as a Poisson process (default 5/s, a
sped-up day), weighted by store size and product popularity. Every 30s one
price changes; every 10s it checks `inventory_current` for items at or below
their reorder level and schedules a delivery. It only *reads* stock: applying
sales and deliveries to Postgres is the streaming job's job (phase 3).

## Design notes

- **Stock is per store** — `inventory_current` is keyed on `(store_id, product_id)`.
- **Negative stock is allowed and flagged**, not clamped: it means sales were
  recorded faster than deliveries, which is information worth keeping.
- **Idempotency ledger** — `processed_lines (invoice_id, line_no)` will let the
  streaming job skip lines from a replayed micro-batch.
- **Invoices are keyed by invoice_id**, not store: three store keys hashed onto a
  single partition, and stock changes add up, so there's no ordering to preserve.

## Layout

```
docker-compose.yml          Kafka + Postgres
sql/init/                   Postgres schema, applied on first start
seeds/                      Product master and stores (CSV)
src/retail/
  catalog.py                Reference data + rules (reorder levels, historical prices)
  events.py                 Event contracts (Pydantic)
  seed.py                   Loads the live store
  simulator/invoices.py     Basket and invoice builder shared by both simulators
  simulator/history.py      Historical invoice files for the batch path
  simulator/live.py         Kafka producer for the streaming path
tests/
```

## Roadmap

1. ~~Foundation~~ — infrastructure, product master, event contracts, simulators
2. Batch — bronze / silver / gold Delta tables from the historical files
3. Streaming — Spark Structured Streaming into Postgres stock and Delta
4. Dashboard — Streamlit: live stock, low-stock alerts, sales, price changes
5. Semantic search and Q&A — embeddings, FastAPI, DuckDB over gold
6. Hardening — data quality checks, monitoring, CI
