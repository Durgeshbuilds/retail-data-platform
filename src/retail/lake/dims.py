"""Dimensions: the product master and stores, loaded from seeds/ into silver."""

import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail.config import SEEDS_DIR
from retail.lake.tables import Lake

log = logging.getLogger(__name__)

PRODUCTS_DDL = (
    "product_id STRING, name STRING, category STRING, brand STRING, unit STRING,"
    " price DECIMAL(10,2), popularity INT"
)
STORES_DDL = "store_id STRING, name STRING, city STRING, opened_on DATE, size_factor DOUBLE"


def build(spark: SparkSession, lake: Lake, seeds_dir: Path = SEEDS_DIR) -> None:
    """Overwrite both dimension tables. They are small, so a full reload is simplest."""
    products = (
        spark.read.option("header", True).schema(PRODUCTS_DDL)
        .csv(str(seeds_dir / "products.csv"))
        .withColumnRenamed("price", "list_price")  # live prices are in Postgres
    )
    stores = (
        spark.read.option("header", True).schema(STORES_DDL)
        .csv(str(seeds_dir / "stores.csv"))
    )
    for name, df, path in [("products", products, lake.silver_products),
                           ("stores", stores, lake.silver_stores)]:
        df = df.withColumn("loaded_at", F.current_timestamp())
        df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(path)
        log.info("dims: %s reloaded (%d rows)", name, df.count())
