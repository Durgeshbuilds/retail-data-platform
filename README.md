# Retail Data Platform

A supermarket data platform: invoices stream through Kafka and Spark into
live per-store stock in Postgres, while batch jobs build a Delta Lake for
history and analytics.

> Work in progress — built phase by phase. Status: **Phase 1 (foundation)**.

## Prerequisites (macOS, Apple Silicon)

| Tool | Version | Install |
|---|---|---|
| Docker Desktop | latest | https://www.docker.com/products/docker-desktop/ — cap memory at ~4 GB |
| Java | 17 | `brew install openjdk@17` |
| Python | 3.12 | `brew install uv` then `uv python install 3.12` |

Spark runs natively (not in Docker) to save memory.

## Run the infrastructure

```bash
make up        # start Kafka (KRaft) and Postgres, wait until healthy
make topics    # create invoices, price-updates, stock-receipts
make psql      # inspect the live-store schema: \dt
```

`make down` stops everything; `make reset` also deletes the data.

## Layout

```
docker-compose.yml     Kafka + Postgres
sql/init/              Postgres schema, applied on first start
```
