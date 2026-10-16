KAFKA_TOPICS = docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092
TOPICS = invoices price-updates stock-receipts

.PHONY: up down reset topics list-topics psql

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
