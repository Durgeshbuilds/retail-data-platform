"""Gold: aggregates for the dashboard and the Q&A layer.

* sales_daily    date x store x product: units, revenue (plus category)
* store_daily    date x store: invoices, revenue, average basket
* sales_monthly  month x store x category: units, revenue

Rebuilt from silver. A full rebuild overwrites each table; with from_date only
the affected dates are replaced (Delta replaceWhere), which is how a daily
schedule would run it. Either way the swap is atomic: readers see the old
version until the new one commits.
"""

import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retail.lake.tables import Lake

log = logging.getLogger(__name__)

REVENUE = "decimal(14,2)"


def sales_daily(items: DataFrame, products: DataFrame) -> DataFrame:
    categories = F.broadcast(products.select("product_id", "category"))
    return (
        items.join(categories, "product_id", "left")
        .withColumn("category", F.coalesce("category", F.lit("Unknown")))
        .groupBy("event_date", "store_id", "product_id", "category")
        .agg(
            F.sum("qty").cast("long").alias("units"),
            F.sum("line_amount").cast(REVENUE).alias("revenue"),
        )
    )


def store_daily(invoices: DataFrame) -> DataFrame:
    return (
        invoices.groupBy("event_date", "store_id")
        .agg(
            F.count("*").alias("invoices"),
            F.sum("total_amount").cast(REVENUE).alias("revenue"),
            F.round(F.avg("line_count"), 2).alias("avg_lines_per_invoice"),
        )
        .withColumn("avg_basket_value", F.round(F.col("revenue") / F.col("invoices"), 2))
    )


def sales_monthly(daily: DataFrame) -> DataFrame:
    return (
        daily.withColumn("month", F.trunc("event_date", "month"))
        .groupBy("month", "store_id", "category")
        .agg(
            F.sum("units").alias("units"),
            F.sum("revenue").cast(REVENUE).alias("revenue"),
        )
    )


def _write(df: DataFrame, path: str, date_col: str, since: date | None) -> None:
    writer = df.write.format("delta").mode("overwrite")
    if since is None:
        writer = writer.option("overwriteSchema", True)
    else:
        writer = writer.option("replaceWhere", f"{date_col} >= '{since.isoformat()}'")
    writer.save(path)


def build(spark: SparkSession, lake: Lake, from_date: date | None = None) -> None:
    items = spark.read.format("delta").load(lake.silver_invoice_items)
    invoices = spark.read.format("delta").load(lake.silver_invoices)
    products = spark.read.format("delta").load(lake.silver_products)
    month_start = from_date.replace(day=1) if from_date else None
    if from_date:
        # Monthly totals need the whole month, so read back to its first day.
        items = items.filter(F.col("event_date") >= F.lit(month_start))
        invoices = invoices.filter(F.col("event_date") >= F.lit(from_date))

    daily = sales_daily(items, products).persist()
    try:
        recent_daily = daily.filter(F.col("event_date") >= F.lit(from_date)) if from_date else daily
        _write(recent_daily, lake.gold_sales_daily, "event_date", from_date)
        _write(store_daily(invoices), lake.gold_store_daily, "event_date", from_date)
        _write(sales_monthly(daily), lake.gold_sales_monthly, "month", month_start)
    finally:
        daily.unpersist()
    log.info("gold: rebuilt %s", f"from {from_date}" if from_date else "in full")
