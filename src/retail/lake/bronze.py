"""Bronze: land raw invoice files in Delta, unchanged.

Uses Structured Streaming with an availableNow trigger as an incremental file
loader: the checkpoint records which files have been read, so each run picks up
only new files and a re-run ingests nothing twice.
"""

import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail.lake.schemas import BRONZE
from retail.lake.tables import Lake, ensure_table

log = logging.getLogger(__name__)


def ingest_files(spark: SparkSession, lake: Lake, source_dir: Path) -> int:
    """Append rows from new files under source_dir/*/*.jsonl.gz. Returns rows added."""
    if not any(source_dir.glob("*/*.jsonl.gz")):
        raise FileNotFoundError(f"no invoice files under {source_dir}; run `make history`")
    ensure_table(spark, lake.bronze_invoices, BRONZE)

    rows = (
        spark.readStream.format("text")
        .load(str(source_dir / "*" / "*.jsonl.gz"))
        .select(
            F.col("value").alias("raw"),
            F.lit("file").alias("source"),
            F.col("_metadata.file_path").alias("source_ref"),
            F.current_timestamp().alias("ingested_at"),
        )
    )
    query = (
        rows.writeStream.format("delta")
        .option("checkpointLocation", lake.checkpoint("bronze_files"))
        .trigger(availableNow=True)
        .start(lake.bronze_invoices)
    )
    query.awaitTermination()
    added = sum(p["numInputRows"] for p in query.recentProgress)
    log.info("bronze: %d new rows", added)
    return added
