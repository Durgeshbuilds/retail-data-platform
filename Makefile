KAFKA_TOPICS = docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092
TOPICS = invoices price-updates stock-receipts

.PHONY: up down reset topics list-topics psql db-migrate install seed history simulate stream stream-check dashboard search-index api quality check peek lake lake-check lake-reset test test-fast lint

up:  ## Start Kafka and Postgres and wait until both are healthy
	docker compose up -d --wait

down:  ## Stop containers, keep data
	docker compose down

reset:  ## Stop containers and delete all data (re-runs sql/init on next up)
	docker compose down -v

topics:  ## Create the Kafka topics (safe to re-run)
	@for t in $(TOPICS); do \
		$(KAFKA_TOPICS) --create --if-not-exists --topic $$t --partitions 3 --replication-factor 1; \
	done

list-topics:
	$(KAFKA_TOPICS) --list

psql:  ## Open a psql shell on the live store
	docker compose exec postgres psql -U retail -d retail

db-migrate:  ## Apply idempotent schema changes to an existing database
	docker compose exec -T -e PGOPTIONS="-c client_min_messages=warning" postgres \
		psql -U retail -d retail -v ON_ERROR_STOP=1 -q \
		-f /docker-entrypoint-initdb.d/02_streaming.sql \
		-f /docker-entrypoint-initdb.d/03_dashboard.sql \
		-f /docker-entrypoint-initdb.d/04_search.sql \
		-f /docker-entrypoint-initdb.d/05_quality.sql

install:  ## Create .venv on Python 3.12 and install dependencies
	uv sync

seed:  ## Load stores, products, price history and opening stock (safe to re-run)
	uv run python -m retail.seed

history:  ## Generate 12 months of daily invoice files (only days not generated yet)
	uv run python -m retail.simulator.history

simulate:  ## Stream live invoices, price changes and restocks to Kafka (Ctrl-C to stop)
	uv run python -m retail.simulator.live

stream:  ## Spark streaming job: Kafka -> Postgres stock/prices/sales + bronze (Ctrl-C to stop)
	uv run python -m retail.streaming.job

stream-check:  ## Verify every Kafka event was applied to Postgres exactly once
	uv run python -m retail.streaming.check

dashboard:  ## Streamlit dashboard on http://localhost:8501
	uv run streamlit run src/retail/dashboard/app.py

search-index:  ## Embed products into pgvector (only changed products are re-embedded)
	uv run python -m retail.search.index

api:  ## FastAPI on http://localhost:8000 (docs at /docs)
	uv run uvicorn retail.api.main:app --port 8000

TOPIC ?= invoices
N ?= 3
peek:  ## Show the newest messages on a topic: make peek TOPIC=price-updates N=5
	uv run python -m retail.peek $(TOPIC) -n $(N)

lake:  ## Batch pipeline: dims -> bronze -> silver -> gold -> reconciliation checks
	uv run python -m retail.lake.run

lake-check:  ## Reconcile counts and revenue across bronze, silver and gold
	uv run python -m retail.lake.run check

lake-reset:  ## Delete the Delta lake and its checkpoints (raw files are kept)
	rm -rf data/lake

quality:  ## Data-quality checks (silver, gold, live, stream); results saved to Postgres
	uv run python -m retail.ops.quality

check: lint test  ## Lint and run every test (what CI runs)

test:  ## All tests, including Spark ones (~40s)
	uv run pytest

test-fast:  ## Tests that don't start Spark
	uv run pytest -m "not spark"

lint:
	uv run ruff check .
