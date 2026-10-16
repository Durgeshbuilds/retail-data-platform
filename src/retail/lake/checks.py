"""Reconciliation: every invoice and rupee in bronze must be accounted for downstream."""

import logging
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail.lake.silver import parse_invoices
from retail.lake.tables import Lake

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Check:
    name: str
    expected: object
    actual: object

    @property
    def ok(self) -> bool:
        return self.expected == self.actual


def reconcile(spark: SparkSession, lake: Lake) -> list[Check]:
    def read(path: str):
        return spark.read.format("delta").load(path)

    parsed = parse_invoices(read(lake.bronze_invoices))
    valid = parsed.filter(F.col("reject_reason").isNull())
    bronze = parsed.agg(
        F.count("*").alias("rows"),
        F.count_if(F.col("reject_reason").isNotNull()).alias("rejected"),
    ).first()
    bronze_valid = valid.dropDuplicates(["invoice_id"]).agg(
        F.count("*").alias("invoices"), F.sum("total_amount").alias("revenue"),
    ).first()

    invoices = read(lake.silver_invoices).agg(
        F.count("*").alias("n"), F.sum("total_amount").alias("revenue")
    ).first()
    items = read(lake.silver_invoice_items).agg(
        F.count("*").alias("n"), F.sum("qty").alias("units"), F.sum("line_amount").alias("revenue")
    ).first()
    daily = read(lake.gold_sales_daily).agg(
        F.sum("units").alias("units"), F.sum("revenue").alias("revenue")
    ).first()
    store = read(lake.gold_store_daily).agg(
        F.sum("invoices").alias("invoices"), F.sum("revenue").alias("revenue")
    ).first()
    monthly = read(lake.gold_sales_monthly).agg(F.sum("revenue").alias("revenue")).first()
    rejects = read(lake.silver_invoice_rejects).count()
    unknown = read(lake.gold_sales_daily).filter(F.col("category") == "Unknown").count()

    log.info("bronze: %d rows, %d rejected", bronze["rows"], bronze["rejected"])
    return [
        Check("silver invoices = distinct valid bronze invoices",
              bronze_valid["invoices"], invoices["n"]),
        Check("silver revenue = bronze revenue", bronze_valid["revenue"], invoices["revenue"]),
        Check("item lines sum to invoice totals", invoices["revenue"], items["revenue"]),
        Check("rejects table = rejected bronze rows", bronze["rejected"], rejects),
        Check("gold sales_daily units = silver units", items["units"], daily["units"]),
        Check("gold sales_daily revenue = silver revenue", items["revenue"], daily["revenue"]),
        Check("gold store_daily invoices = silver invoices", invoices["n"], store["invoices"]),
        Check("gold store_daily revenue = silver revenue", invoices["revenue"], store["revenue"]),
        Check("gold sales_monthly revenue = silver revenue", items["revenue"], monthly["revenue"]),
        Check("every sold product is in the product master", 0, unknown),
    ]
