"""Where each Delta table lives, plus small helpers shared by the jobs."""

from dataclasses import dataclass
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


@dataclass(frozen=True)
class Lake:
    """Paths of the lake's tables under one root directory.

    Tables are addressed by path rather than through a metastore, which keeps the
    local setup to "a folder on disk" and makes tests trivial to isolate.
    """

    root: Path

    def _path(self, *parts: str) -> str:
        return str(self.root.joinpath(*parts))

    @property
    def bronze_invoices(self) -> str:
        return self._path("bronze", "invoices")

    @property
    def silver_invoices(self) -> str:
        return self._path("silver", "invoices")

    @property
    def silver_invoice_items(self) -> str:
        return self._path("silver", "invoice_items")

    @property
    def silver_invoice_rejects(self) -> str:
        return self._path("silver", "invoice_rejects")

    @property
    def silver_products(self) -> str:
        return self._path("silver", "products")

    @property
    def silver_stores(self) -> str:
        return self._path("silver", "stores")

    @property
    def gold_sales_daily(self) -> str:
        return self._path("gold", "sales_daily")

    @property
    def gold_store_daily(self) -> str:
        return self._path("gold", "store_daily")

    @property
    def gold_sales_monthly(self) -> str:
        return self._path("gold", "sales_monthly")

    def checkpoint(self, name: str) -> str:
        return self._path("_checkpoints", name)


def ensure_table(spark: SparkSession, path: str, schema: StructType) -> None:
    DeltaTable.createIfNotExists(spark).location(path).addColumns(schema).execute()


def insert_new(spark: SparkSession, path: str, source: DataFrame, keys: list[str]) -> None:
    """Insert rows whose key is not in the table yet; existing keys are left alone.

    An insert-only MERGE is idempotent: replaying the same batch, or receiving the
    same event twice, never creates a second row.
    """
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(source.alias("s"), condition)
        .whenNotMatchedInsertAll()
        .execute()
    )
