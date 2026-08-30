"""
orders_cleaning.py
------------------
Transformation module for cleansing, normalizing, and validating
raw order records for the FMCG Silver layer.
"""

import sys

# Add project root to Python path for imports
project_root = "/Workspace/Users/mahnoortauseef2022@gmail.com/fmcg_data_lakehouse"
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.config.settings import PipelineConfig


def clean_orders_data(df_raw: DataFrame) -> DataFrame:
    """
    Cleans raw order transactions by filtering invalid quantities, 
    validating customer keys, scrubbing date prefixes, parsing mixed date 
    formats, and removing duplicate records.
    """
    # 1. Filter out null or non-positive order quantities
    df_clean = df_raw.filter(
        F.col("order_qty").isNotNull() & (F.col("order_qty") > 0)
    )

    # 2. Flag non-numeric customer IDs for downstream Quarantine routing
    df_clean = df_clean.withColumn(
        "is_valid_customer",
        F.col("customer_id").rlike(r"^[0-9]+$")
    )

    # 3. Strip weekday prefixes (e.g., "Tuesday, July 01, 2025" -> "July 01, 2025")
    df_clean = df_clean.withColumn(
        "order_placement_date",
        F.regexp_replace(
            F.col("order_placement_date"),
            r"^\s*(?:Mon|Tue|Tues|Wed|Thu|Thur|Thurs|Fri|Sat|Sun)[a-z]*\.?,?\s*",
            ""
        )
    )

    # 4. Parse mixed date string formats into standard PySpark DateType
    df_clean = df_clean.withColumn(
        "order_placement_date",
        F.coalesce(
            F.try_to_date(F.col("order_placement_date"), "yyyy/MM/dd"),
            F.try_to_date(F.col("order_placement_date"), "dd-MM-yyyy"),
            F.try_to_date(F.col("order_placement_date"), "dd/MM/yyyy"),
            F.try_to_date(F.col("order_placement_date"), "MMMM dd, yyyy")
        )
    )

    # 5. Remove exact duplicates across key grain columns
    df_clean = df_clean.dropDuplicates([
        "order_id", 
        "order_placement_date", 
        "customer_id", 
        "product_id", 
        "order_qty"
    ])

    # 6. Enforce schema consistency by casting key fields
    return df_clean.withColumn("product_id", F.col("product_id").cast("string"))


def ingest_bronze_autoloader(config: PipelineConfig) -> None:
    """
    Reads incoming S3 files incrementally using Databricks Auto Loader 
    and streams them directly into the Bronze Delta table.
    """
    checkpoint_path = f"{config.base_path}/_checkpoints/bronze_orders"

    query = (
        config.spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .load(config.landing_path)
        .withColumn("read_timestamp", F.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(config.bronze_table)
    )
    
    # Force the driver to wait until the stream has completed processing all files
    query.awaitTermination()