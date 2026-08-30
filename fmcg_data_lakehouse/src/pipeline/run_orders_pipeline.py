import logging
import os
import sys

# Dynamic root resolution (Works in Databricks Notebooks, REPLs, & .py Jobs)
try:
    _current_dir = os.path.dirname(os.path.abspath(__file__))
    _project_root = os.path.abspath(os.path.join(_current_dir, "..", ".."))
except NameError:
    # Fallback for interactive notebook environments where __file__ is undefined
    _current_dir = os.getcwd()
    _project_root = os.path.abspath(os.path.join(_current_dir, "..", ".."))

if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from src.config.settings import PipelineConfig
from src.transformations.orders_cleaning import (
    clean_orders_data,
    ingest_bronze_autoloader,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def deduplicate_source(df, keys: list, order_by_col: str):
    """Deduplicates a DataFrame deterministically using the latest timestamp."""
    window_spec = Window.partitionBy(*keys).orderBy(col(order_by_col).desc())
    return (
        df.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


def merge_to_delta(
    spark: SparkSession,
    df,
    target_table_name: str,
    join_condition: str,
    cluster_keys: list = None,
):
    """Executes an idempotent Delta MERGE with schema evolution and optimization."""
    try:
        if not spark.catalog.tableExists(target_table_name):
            logging.info(f"Initializing target table: {target_table_name}")
            writer = df.write.format("delta").mode("append").option("mergeSchema", "true")
            writer.saveAsTable(target_table_name)
            
            # Apply Liquid Clustering on initial creation if supported/configured
            if cluster_keys:
                cluster_str = ", ".join(cluster_keys)
                spark.sql(f"ALTER TABLE {target_table_name} CLUSTER BY ({cluster_str})")
        else:
            target_table = DeltaTable.forName(spark, target_table_name)
            (
                target_table.alias("target")
                .merge(df.alias("source"), join_condition)
                .withSchemaEvolution()
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logging.info(f"Successfully merged data into {target_table_name}")
            
    except Exception as e:
        logging.error(f"Failed to merge into {target_table_name}: {str(e)}")
        raise e


def run_silver_pipeline():
    spark = SparkSession.builder.getOrCreate()
    config = PipelineConfig(spark, catalog="fmcg", data_source="orders")

    logging.info("Ingesting landing files into Bronze via Auto Loader...")
    ingest_bronze_autoloader(config)

    logging.info("Reading ingested Bronze table...")
    raw_df = spark.read.table(config.bronze_table)

    logging.info("Cleaning order records...")
    transformed_df = clean_orders_data(raw_df)

    # Standardize column renaming & drop flag
    base_df = transformed_df.withColumnRenamed("product_id", "product_code")

    # Split Clean vs Quarantined records
    clean_df = base_df.filter(col("is_valid_customer") == True).drop("is_valid_customer")
    quarantine_df = base_df.filter(col("is_valid_customer") == False).drop("is_valid_customer")

    # Deterministic Deduplication over business keys using ingestion timestamp
    dedup_keys = ["order_id", "order_placement_date"]
    ts_col = "ingested_timestamp" if "ingested_timestamp" in base_df.columns else "order_placement_date"

    clean_records = deduplicate_source(clean_df, keys=dedup_keys, order_by_col=ts_col)
    quarantine_records = deduplicate_source(quarantine_df, keys=dedup_keys, order_by_col=ts_col)

    join_cond = (
        "target.order_id = source.order_id AND "
        "target.order_placement_date = source.order_placement_date"
    )

    logging.info("Merging clean records into Silver...")
    merge_to_delta(
        spark,
        clean_records,
        config.silver_table,
        join_cond,
        cluster_keys=["order_placement_date"],
    )

    logging.info("Merging quarantined records into Silver Quarantine...")
    merge_to_delta(
        spark,
        quarantine_records,
        config.silver_quarantine_table,
        join_cond,
        cluster_keys=["order_placement_date"],
    )

    logging.info(f"Silver Pipeline Execution Completed for {config.data_source}!")


if __name__ == "__main__":
    run_silver_pipeline()