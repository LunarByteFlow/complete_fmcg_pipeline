import logging
import os
import sys

# Dynamic root resolution (Works in Databricks Notebooks, REPLs, & .py Jobs)
try:
    _current_dir = os.path.dirname(os.path.abspath(__file__))
    _project_root = os.path.abspath(os.path.join(_current_dir, "..", ".."))
except NameError:
    _current_dir = os.getcwd()
    _project_root = os.path.abspath(os.path.join(_current_dir, "..", ".."))

if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config.settings import PipelineConfig
from src.transformations.pricing_cleaning import (
    clean_pricing_data,
    build_latest_price_per_year,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def ingest_bronze_pricing(spark: SparkSession, config: PipelineConfig) -> None:
    """Reads raw pricing CSV drops from S3 into the Bronze layer (batch read,
    not streamed via Auto Loader, since pricing updates arrive far less
    frequently than order transactions)."""
    raw_path = f"{config.base_path}/*.csv"
    logging.info(f"Reading raw pricing files from {raw_path}")

    df = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(raw_path)
        .withColumn("read_timestamp", F.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
    )

    df.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).mode("overwrite").saveAsTable(config.bronze_table)


def run_pricing_pipeline():
    spark = SparkSession.builder.getOrCreate()
    config = PipelineConfig(spark, catalog="fmcg", data_source="gross_price")

    logging.info("Ingesting raw pricing files into Bronze...")
    ingest_bronze_pricing(spark, config)

    logging.info("Reading Bronze pricing table...")
    df_bronze = spark.table(config.bronze_table)

    logging.info("Reading Silver products table for product_code enrichment...")
    df_products = spark.table("fmcg.silver.products")

    logging.info("Cleaning & enriching pricing records...")
    df_silver = clean_pricing_data(df_bronze, df_products)

    df_silver.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(
        config.silver_table
    )
    logging.info(f"Silver pricing written to {config.silver_table}")

    # Gold: full history, one row per product_code / month
    logging.info("Writing Gold dim_gross_price (full history)...")
    df_gold = df_silver.select("product_id", "product_code", "month", "gross_price")
    gold_table_name = f"{config.catalog}.{config.gold_schema}.sb_dim_gross_price"
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("overwriteSchema", "true").mode("overwrite").saveAsTable(
        gold_table_name
    )

    # Gold: latest non-zero price per product_code / year, merged into parent
    logging.info("Building latest-price-per-year and merging into dim_gross_price...")
    df_latest_price = build_latest_price_per_year(df_gold)

    parent_table_name = f"{config.catalog}.{config.gold_schema}.dim_gross_price"
    if not spark.catalog.tableExists(parent_table_name):
        logging.info(f"Initializing {parent_table_name}")
        df_latest_price.write.format("delta").mode("append").saveAsTable(
            parent_table_name
        )
    else:
        target = DeltaTable.forName(spark, parent_table_name)
        (
            target.alias("target")
            .merge(
                df_latest_price.alias("source"),
                "target.product_id = source.product_id AND target.year = source.year",
            )
            .whenMatchedUpdate(
                set={
                    "product_code": "source.product_code",
                    "price_inr": "source.price_inr",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "product_id": "source.product_id",
                    "product_code": "source.product_code",
                    "price_inr": "source.price_inr",
                    "year": "source.year",
                }
            )
            .execute()
        )

    logging.info("Pricing pipeline execution completed successfully!")


if __name__ == "__main__":
    run_pricing_pipeline()