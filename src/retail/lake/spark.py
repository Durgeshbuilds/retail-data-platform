"""SparkSession factory tuned for a 16 GB laptop, with Delta Lake enabled."""

from collections.abc import Sequence

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_spark(
    app_name: str,
    master: str = "local[4]",
    driver_memory: str = "3g",
    extra_packages: Sequence[str] = (),
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        # The default of 200 shuffle partitions is sized for clusters; locally it
        # just means 200 tiny tasks per shuffle.
        .config("spark.sql.shuffle.partitions", "8")
        # Store and compute timestamps in UTC; convert to store time explicitly.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.showConsoleProgress", "false")
    )
    # Adds the Delta jars (and any extra Maven packages) matching the installed delta-spark.
    spark = configure_spark_with_delta_pip(builder, extra_packages=list(extra_packages))
    spark = spark.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
