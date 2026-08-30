import os
import sys
import pytest
from pyspark.sql.types import StringType, StructField, StructType

# Databricks import path resolution
try:
    _current_dir = os.path.dirname(os.path.abspath(__file__))
    _project_root = os.path.abspath(os.path.join(_current_dir, ".."))
except NameError:
    # Handle running inside interactive Databricks notebooks
    _project_root = os.path.abspath(os.path.join(os.getcwd(), ".."))

if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.pipeline.run_orders_pipeline import deduplicate_source


@pytest.fixture(scope="session")
def spark():
    """Uses existing Databricks SparkSession or builds fallback for local execution."""
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    except Exception:
        from pyspark.sql import SparkSession
        return SparkSession.builder.master("local[1]").appName("unit-testing").getOrCreate()


def test_deduplicate_source_keeps_latest_record(spark):
    """Validates that windowed deduplication correctly retains only the newest record."""
    schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("order_placement_date", StringType(), True),
            StructField("ingested_timestamp", StringType(), True),
            StructField("val", StringType(), True),
        ]
    )

    data = [
        ("ORD123", "2026-08-01", "2026-08-01 10:00:00", "old_value"),
        ("ORD123", "2026-08-01", "2026-08-01 12:00:00", "latest_value"),  # Should win
    ]

    input_df = spark.createDataFrame(data, schema)

    # Execute transformation
    result_df = deduplicate_source(
        df=input_df,
        keys=["order_id", "order_placement_date"],
        order_by_col="ingested_timestamp",
    )

    # Assertions
    assert result_df.count() == 1
    assert result_df.collect()[0]["val"] == "latest_value"
