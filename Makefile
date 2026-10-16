KAFKA_TOPICS = docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092
TOPICS = invoices price-updates stock-receipts

.PHONY: up down reset topics list-topics psql install seed history simulate peek test lint

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

install:  ## Create .venv on Python 3.12 and install dependencies
	uv sync

seed:  ## Load stores, products, price history and opening stock (safe to re-run)
	uv run python -m retail.seed

history:  ## Generate 12 months of historical invoices into data/raw/invoices
	uv run python -m retail.simulator.history --overwrite

simulate:  ## Stream live invoices, price changes and restocks to Kafka (Ctrl-C to stop)
	uv run python -m retail.simulator.live

TOPIC ?= invoices
peek:  ## Show the latest 3 messages on a topic: make peek TOPIC=price-updates
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 --topic $(TOPIC) --from-beginning \
		--property print.key=true --max-messages 3 --timeout-ms 5000

test:
	uv run pytest

lint:
	uv run ruff check .
