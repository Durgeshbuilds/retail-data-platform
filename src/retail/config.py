"""Connection settings and shared paths, read from the environment with local defaults."""

import os
from pathlib import Path

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://retail:retail@localhost:5432/retail")
# Read-only role (sql/init/03_dashboard.sql) for the dashboard and Q&A layer.
POSTGRES_READER_DSN = os.getenv(
    "POSTGRES_READER_DSN", "postgresql://retail_reader:retail_reader@localhost:5432/retail"
)

# FastAPI service (make api), called by the dashboard's Ask tab.
API_URL = os.getenv("RETAIL_API_URL", "http://localhost:8000")

TOPIC_INVOICES = "invoices"
TOPIC_PRICE_UPDATES = "price-updates"
TOPIC_STOCK_RECEIPTS = "stock-receipts"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SEEDS_DIR = PROJECT_ROOT / "seeds"
DATA_DIR = PROJECT_ROOT / "data"
RAW_INVOICES_DIR = DATA_DIR / "raw" / "invoices"
LAKE_DIR = DATA_DIR / "lake"
