"""Connection settings and shared paths, read from the environment with local defaults."""

import os
from pathlib import Path

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://retail:retail@localhost:5432/retail")

TOPIC_INVOICES = "invoices"
TOPIC_PRICE_UPDATES = "price-updates"
TOPIC_STOCK_RECEIPTS = "stock-receipts"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SEEDS_DIR = PROJECT_ROOT / "seeds"
DATA_DIR = PROJECT_ROOT / "data"
RAW_INVOICES_DIR = DATA_DIR / "raw" / "invoices"
LAKE_DIR = DATA_DIR / "lake"
