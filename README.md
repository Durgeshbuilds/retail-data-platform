# Retail Data Platform

A supermarket data platform built end to end on a laptop: invoices stream through
**Kafka** and **Spark Structured Streaming** into live per-store stock in
**Postgres**, batch jobs build a **Delta Lake** (bronze → silver → gold) for
history, a **Streamlit** dashboard shows both, and a **FastAPI** service answers
questions like *"How many milks were sold last quarter?"* using hybrid search over
**pgvector**.

**Stack:** Python 3.12 · PySpark 4.0 ·
Delta Lake 4.0 · Kafka 4.0 (KRaft) · Postgres 17 + pgvector · DuckDB · FastAPI · Streamlit

## Architecture

```mermaid
flowchart LR
    sim[Live simulator] -->|invoices, price-updates,<br/>stock-receipts| kafka[(Kafka)]
    hist[History generator] -->|daily JSONL files| raw[(Raw files)]
    kafka --> stream[Spark Structured Streaming]
    stream -->|one transaction per batch| pg[(Postgres<br/>live stock, prices, sales)]
    stream -->|idempotent append| bronze
    raw --> bronze[Bronze]
    subgraph lake [Delta Lake]
        bronze --> silver[Silver] --> gold[Gold]
    end
    pg --> dash[Streamlit dashboard]
    gold -->|DuckDB| dash
    gold -->|DuckDB| api[FastAPI /ask, /search]
    pg -->|read-only, pgvector| api
    api --> dash
    dq[Data-quality checks] -.-> silver & gold & pg
```

## What this demonstrates

- **Exactly-once effects without distributed transactions.** A replayed Spark
  micro-batch changes nothing: Postgres writes are claimed in ledgers inside one
  transaction, and Delta appends are keyed on the batch id. Tested with `kill -9`
  mid-stream ([details](#streaming-kafka--spark--postgres--delta)).
- **One event contract for batch and streaming.** Historical files and Kafka carry
  the same validated JSON, so bronze ingests both with one schema.
- **Numbers that prove themselves.** Reconciliation checks tie gold back to silver
  and bronze to the paisa, Postgres stock back to every Kafka event, and a
  data-quality suite with planted-fault tests guards each layer.
- **Honest question answering.** Dates are computed in code, products resolved by
  hybrid search (literal first, embeddings as a fallback), and every answer shows
  the products it counted and the SQL it ran.
- **Laptop-sized engineering.** 16 GB MacBook Air: Spark runs natively with 8
  shuffle partitions, Docker holds only Kafka and Postgres, DuckDB serves reads
  without starting a JVM.

## Prerequisites (macOS, Apple Silicon)

| Tool | Version | Install |
|---|---|---|
| Docker Desktop | latest | https://www.docker.com/products/docker-desktop/ — cap memory at ~4 GB |
| Java | 17 | `brew install openjdk@17` |
| Python | 3.12 | `brew install uv` then `uv python install 3.12` |

Spark runs natively (not in Docker) to save memory.

## Quick start

```bash
make up            # Kafka (KRaft) + Postgres with pgvector, waits until healthy
make topics        # invoices, price-updates, stock-receipts
make install       # .venv on Python 3.12
make seed          # 3 stores, 200 products, 12 months of price history, opening stock
make search-index  # embed the product catalogue into pgvector
make history       # ~370k historical invoices -> data/raw/invoices/YYYY-MM/STORE-DATE.jsonl.gz
make lake          # Spark: dims -> bronze -> silver -> gold -> reconciliation (~35s)
make quality       # data-quality checks; results feed the dashboard's Health tab

# then, each in its own terminal
make stream        # Spark Structured Streaming: Kafka -> Postgres + bronze
make simulate      # live invoices, price changes and restocks (Ctrl-C to stop)
make api           # FastAPI on http://localhost:8000 (docs at /docs)
make dashboard     # Streamlit on http://localhost:8501
```

Also: `make stream-check` (Kafka vs Postgres reconciliation), `make peek TOPIC=invoices N=3`,
`make check` (lint + all tests), `make test-fast` (skip Spark tests). `make down` stops
everything; `make reset` also deletes the data. For a database created by an
earlier version of this repo, run `make db-migrate` once.

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
bronze layer ingests the batch and streaming paths with one schema. There is one
file per store per day, like a nightly POS export. A store-day's content depends
only on `--seed`, the store and the date, and existing files are never rewritten:
re-running `make history` on a later day adds just the new days. (Bronze tracks
files by path, so a rewritten file would silently never be re-read.)

Prices are a fixed function of the month: list prices in `products.csv` apply to
October 2026 and drift ~0.5% a month either side, so the same month always gets
the same price, and the seeded `product_prices` history matches the invoices.

**Live simulator** — invoices arrive as a Poisson process (default 5/s, a
sped-up day), weighted by store size and product popularity. Every 30s one
price changes; every 10s it checks `inventory_current` for items at or below
their reorder level and schedules a delivery. It only *reads* stock: applying
sales and deliveries to Postgres is the streaming job's job (phase 3).

## Lakehouse (Delta Lake, PySpark 4.0)

Path-based Delta tables under `data/lake/`, built by
[`src/retail/lake/`](src/retail/lake) (`python -m retail.lake.run [steps]`).

| Layer | Table | Grain | How it's written |
|---|---|---|---|
| Bronze | `bronze/invoices` | one raw JSON event | Append. Files are loaded by a Structured Streaming `availableNow` query, whose checkpoint remembers which files were read |
| Silver | `silver/invoices` | invoice | Insert-only `MERGE` on `invoice_id` |
| Silver | `silver/invoice_items` | invoice line | Insert-only `MERGE` on `(invoice_id, line_no)` |
| Silver | `silver/invoice_rejects` | rejected event + reason | Idempotent append (`txnAppId` / `txnVersion`) |
| Silver | `silver/products`, `silver/stores` | dimension rows | Full reload from `seeds/` |
| Gold | `gold/sales_daily` | date × store × product | Overwrite, or `replaceWhere` from `--from-date` |
| Gold | `gold/store_daily` | date × store | Invoices, revenue, average basket |
| Gold | `gold/sales_monthly` | month × store × category | Units, revenue |

- **Incremental and re-runnable.** Bronze reads only new files and silver reads
  only new bronze rows (Delta as a streaming source). Re-running any step is safe.
- **Validation, not silent drops.** Silver rejects unparseable JSON, unknown
  `schema_version`, missing keys, non-positive qty/price, duplicate `line_no`
  and totals that don't equal their lines, recording the reason for each.
- **Store-local dates.** `event_date` is the IST calendar date, so a sale at
  00:30 IST lands on the right day even though it is 19:00 UTC the day before.
- **Reconciliation** (`make lake-check`) proves nothing was lost or double
  counted: distinct valid bronze invoices = silver invoices, line amounts sum to
  invoice totals, and every gold table sums back to silver, to the paisa.
- **Laptop tuning.** `local[4]`, 3 GB driver, 8 shuffle partitions. Tables are
  unpartitioned: ~40 MB per layer would only become many tiny files if split by day.

## Streaming (Kafka → Spark → Postgres + Delta)

[`src/retail/streaming/job.py`](src/retail/streaming/job.py) runs one Structured
Streaming query over all three topics with a 5s trigger. Each micro-batch:

1. appends invoice events to `bronze/invoices` (`source = 'kafka'`), so the batch
   pipeline picks streamed sales up with no extra code;
2. parses invoices with the **same validation code as silver**;
3. applies sales, deliveries and price changes to Postgres in **one transaction**.

Spark commits a batch's Kafka offsets only after it succeeds, so a crash means
the batch is replayed. Every write is therefore idempotent:

| Event | Postgres effect | Why a replay is harmless |
|---|---|---|
| Invoice line | `stock_qty -= qty`, `live_sales_daily += units, revenue` | Line is first claimed in `processed_lines (invoice_id, line_no)`; only lines claimed by this statement move stock (one statement, data-modifying CTEs) |
| Stock receipt | `stock_qty += qty` | `stock_receipts.receipt_id` is the ledger; only newly inserted receipts move stock |
| Price update | Close open `product_prices` row, open a new one, update `current_price` | Applied only if newer than the open row, so replays *and* out-of-order changes are skipped |
| Bronze append | Delta append | `txnAppId` + `txnVersion = batch_id`: Delta skips a batch it already committed |

Measured on a MacBook Air: ~2s from checkout to Postgres at 5 invoices/s, each
batch taking ~1.5s. Tested by `kill -9` mid-stream, selling while it was down,
and restarting: `make stream-check` confirms every Kafka invoice and receipt
was applied exactly once and that, for all 600 store-product rows,
`stock = opening − sold + received`.

The job is the only writer of `inventory_current`, so transactions can't
deadlock; with several writers you would lock rows in key order first.

## Dashboard (Streamlit)

![Live tab](docs/images/dashboard-live.png)

`make dashboard` → http://localhost:8501. One filter row (stores, history
range) scopes the Live and History tabs.

- **Live** (Postgres, refreshes every 5s): revenue, invoices and average basket
  today; revenue by store; top products; stock alerts (⚠️ low, ⛔ out or
  negative); recent price changes with old → new price.
- **History** (gold Delta tables via **DuckDB**, no Spark): monthly revenue by
  store, daily revenue for 90 days, revenue by category. Ends at yesterday, since
  today is partial and lives on the Live tab.

It connects as **`retail_reader`**, a role that is read-only at the database
level (`default_transaction_read_only`, 5s `statement_timeout`), the same role
the Q&A layer uses.

Charts follow one set of rules: each store keeps one colour everywhere (a
palette checked for colour-blind separation in light and dark mode), charts not
about stores use a neutral colour no store uses, status is always icon + label,
and every chart has a table view. Dark mode uses its own colour steps rather
than an automatic inversion.

<details><summary>History tab, dark mode</summary>

![History tab, dark mode](docs/images/dashboard-history-dark.png)
</details>

## Semantic search and Q&A (no LLM)

![Ask tab](docs/images/dashboard-ask.png)

Ask "How many milks were sold last quarter?" in the dashboard's **Ask** tab, or
`POST /ask` on the API. Nothing is generated by a model:

1. **Parse** ([`qa/parse.py`](src/retail/qa/parse.py)): rules pick out the intent
   (sales, top products/categories, stock, price), metric (units/revenue), stores
   and period. Dates are resolved in code ([`qa/periods.py`](src/retail/qa/periods.py)):
   "last quarter" is Q3 2026, 1 Jul – 30 Sep, and the answer says so.
2. **Resolve the subject** ([`search/resolve.py`](src/retail/search/resolve.py)) with
   hybrid retrieval, most precise signal first: category name → brand → stemmed
   full-text on product names → embedding similarity (pgvector, `bge-small-en-v1.5`
   via fastembed on CPU). When name matches span categories, only the category
   closest in meaning is kept: "milk" counts toned, full-cream and soya milk and
   lists Cadbury Dairy Milk as *excluded*. The user can untick or re-tick any product.
3. **Query** ([`qa/engine.py`](src/retail/qa/engine.py)): a fixed SQL template runs in
   DuckDB over the gold Delta tables **and** Postgres (attached read-only as
   `retail_reader`) in one statement. Days before the last gold build come from
   gold, later days from `live_sales_daily`, so nothing is double counted.
4. **Answer** with the sentence, a per-product breakdown, the matched and excluded
   products, notes (e.g. "fuzzy match, check these") and the exact SQL and parameters.

Why hybrid: on its own, the embedding model ranked butter and milk chocolate
alongside real milks for "milk", and potatoes above India Gate rice for "rice".
Literal matches are precise; embeddings only fill in when nothing matches literally.

Postgres runs a small custom image (`docker/postgres`): the same Alpine Postgres 17
with pgvector compiled in, because switching an existing data volume to the
Debian-based pgvector image would change text collation (musl vs glibc).

## Data quality and monitoring

`make quality` ([`ops/quality.py`](src/retail/ops/quality.py)) runs a dozen checks
in a few seconds (DuckDB over Delta, plus Postgres) and stores the results in
`dq_results`. **fail** means the data is wrong and exits non-zero for a scheduler
to alert on; **warn** means it needs a look.

| Layer | Check | Level |
|---|---|---|
| Silver | No null keys or values; every product and store exists | fail |
| Silver | Sale prices within 50–150% of list price; no rejected invoices | warn |
| Gold | Includes yesterday (freshness) | warn |
| Gold | No missing store-days in the last 90 days | fail |
| Gold | Daily invoice volume within 3σ of the 28-day norm | warn |
| Gold | Gold revenue equals silver | fail |
| Live | Stock = opening − sold + received; price history contiguous | fail |
| Live | No negative stock | warn |
| Stream | Fewer than 1,000 messages behind Kafka; job running if messages wait | warn |

Each check has a test that plants the fault it exists to catch. The dashboard's
**Health** tab shows stream lag per topic (Spark checkpoint offsets vs Kafka high
watermarks), when the stream last committed a batch, when gold was built, and the
latest quality run.

## Operations

| Situation | What to do |
|---|---|
| Daily routine | `make history` (new days only) → `make lake` → `make quality` |
| Health tab shows stream lag growing | Is `make stream` running? It resumes from its checkpoint and catches up in `--max-offsets` steps |
| Streaming job crashed | Just restart it: the unfinished batch is replayed and the ledgers skip what was already applied |
| Gold looks stale | `make lake`; for a quick refresh, `uv run python -m retail.lake.run gold --from-date YYYY-MM-DD` |
| A quality check fails | Its row in the Health tab names an example (product, store-day); `make lake-check` and `make stream-check` give the full reconciliation |
| Start over | `make reset && make lake-reset`, then the quick start |

Deleting the streaming checkpoint is safe: Spark then numbers batches from 0
again, but bronze appends are keyed on the checkpoint's own query id as well as the
batch id, so no rows are skipped, and Postgres's ledgers prevent double counting.

## CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs `ruff` and the full test
suite (77 tests: Spark, Postgres and embedding-model tests included) against a
pgvector Postgres service, with Spark packages and the model cached between runs.
Kafka isn't needed: streaming tests feed hand-built batches to the foreachBatch
function.

## Design notes

- **Stock is per store** — `inventory_current` is keyed on `(store_id, product_id)`.
- **Negative stock is allowed and flagged**, not clamped: it means sales were
  recorded faster than deliveries, which is information worth keeping.
- **Idempotency ledger** — `processed_lines (invoice_id, line_no)` lets the
  streaming job skip lines from a replayed micro-batch.
- **Invoices are keyed by invoice_id**, not store: three store keys hashed onto a
  single partition, and stock changes add up, so there's no ordering to preserve.

## Layout

```
docker-compose.yml          Kafka + Postgres (pgvector)
sql/init/                   Postgres schema, applied on first start
seeds/                      Product master and stores (CSV)
src/retail/
  catalog.py                Reference data + rules (reorder levels, historical prices)
  events.py                 Event contracts (Pydantic)
  seed.py                   Loads the live store
  simulator/invoices.py     Basket and invoice builder shared by both simulators
  simulator/history.py      Historical invoice files for the batch path
  simulator/live.py         Kafka producer for the streaming path
  lake/                     Spark + Delta: bronze, silver, dims, gold, checks, run
  streaming/job.py          Kafka -> Postgres + bronze (Structured Streaming)
  streaming/live_store.py   Idempotent Postgres writes for one micro-batch
  streaming/check.py        Kafka vs Postgres reconciliation
  dashboard/                Streamlit app, read-only queries (Postgres + DuckDB), charts
  search/                   Embeddings (fastembed), pgvector indexing, hybrid resolver
  qa/                       Period resolver, question parser, query templates, answers
  api/main.py               FastAPI: /search, /ask
  ops/                      Data-quality checks, stream lag and freshness monitoring
  peek.py                   Newest messages on a topic
docker/postgres/            Postgres 17 (Alpine) + pgvector image
tests/                      Spark and Postgres tests use throwaway lakes and databases
.github/workflows/ci.yml    Lint + full test suite
```


Possible next steps: free-form questions via a local LLM (Ollama, text-to-SQL on the
read-only role), MinIO/S3 object storage for the lake, and an orchestrator
(Airflow or Dagster) in place of the Makefile routine.
