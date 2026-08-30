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
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.config.settings import PipelineConfig

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def run_gold_pipeline():
    spark = SparkSession.builder.getOrCreate()
    config = PipelineConfig(spark, catalog="fmcg", data_source="orders")

    logging.info("Building Gold Dimensional Models...")

    # Load clean Silver entities
    orders_df = spark.table(f"{config.catalog}.silver.orders")
    customers_df = spark.table(f"{config.catalog}.silver.customers")
    products_df = spark.table(f"{config.catalog}.silver.products")

    # Deduplicate Orders deterministically before joining
    dedup_window = Window.partitionBy("order_id", "order_placement_date").orderBy(
        F.col("order_placement_date").desc()
    )
    orders_dedup_df = (
        orders_df.withColumn("rn", F.row_number().over(dedup_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Build Fact Table: Enterprise Resilient Left Join Pattern
    gold_fact_df = (
        orders_dedup_df.alias("o")
        .join(
            customers_df.alias("c"),
            F.col("o.customer_id") == F.col("c.customer_id"),
            "left",
        )
        .join(
            products_df.alias("p"),
            F.col("o.product_code") == F.col("p.product_id"),
            "left",
        )
        .select(
            F.col("o.order_id"),
            F.col("o.order_placement_date"),
            F.coalesce(F.col("o.customer_id"), F.lit("-1")).alias("customer_id"),
            F.coalesce(F.col("o.product_code"), F.lit("UNKNOWN")).alias("product_id"),
            F.col("o.order_qty"),
            F.coalesce(F.col("c.customer_name"), F.lit("Unassigned")).alias("customer_name"),
            F.coalesce(F.col("c.city"), F.lit("Unknown")).alias("city"),
            F.coalesce(F.col("c.market"), F.lit("Unknown")).alias("market"),
            F.coalesce(F.col("p.product"), F.lit("Unassigned")).alias("product"),
            F.coalesce(F.col("p.category"), F.lit("Unassigned")).alias("category"),
            F.coalesce(F.col("p.division"), F.lit("Unassigned")).alias("division"),
        )
    )

    gold_fact_table_name = config.gold_parent_table
    join_condition = (
        "target.order_id = source.order_id AND "
        "target.order_placement_date = source.order_placement_date"
    )

    # Execute Merge & Initialize target table cleanly
    if not spark.catalog.tableExists(gold_fact_table_name):
        logging.info(f"Initializing Gold Fact Table: {gold_fact_table_name}")
        gold_fact_df.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(gold_fact_table_name)
    else:
        target_gold = DeltaTable.forName(spark, gold_fact_table_name)
        (
            target_gold.alias("target")
            .merge(gold_fact_df.alias("source"), join_condition)
            .withSchemaEvolution()
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # Optimize table file sizes after pipeline completion
    logging.info(f"Optimizing Gold Table layout: {gold_fact_table_name}")
    spark.sql(
        f"OPTIMIZE {gold_fact_table_name} ZORDER BY (order_placement_date, customer_id)"
    )

    logging.info(f"Gold Layer Modeling Completed: {gold_fact_table_name}")


if __name__ == "__main__":
    run_gold_pipeline()