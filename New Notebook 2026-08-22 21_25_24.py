# Databricks notebook source
# DBTITLE 1,Restart Python kernel
# Restart Python to reload the updated orders_cleaning.py module
dbutils.library.restartPython()

# COMMAND ----------

import sys
project_root = "/Workspace/Users/mahnoortauseef2022@gmail.com/fmcg_data_lakehouse"
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from pyspark.sql import SparkSession
from src.config.settings import PipelineConfig
from src.transformations.orders_cleaning import clean_orders_data, ingest_bronze_autoloader

spark = SparkSession.builder.getOrCreate()
config = PipelineConfig(spark, catalog="fmcg", data_source="orders")

# 1. Ingest raw landing files using Auto Loader (Incremental)
print("Ingesting landing files into Bronze via Auto Loader...")
ingest_bronze_autoloader(config)

# 2. Read transformed Bronze data
print("Reading Bronze table...")
raw_df = spark.read.table(config.bronze_table)

# 3. Clean Data & Validate Rules
print("Applying cleaning & quality checks...")
transformed_df = clean_orders_data(raw_df)

# 4. Route Clean vs. Quarantined Records
clean_records = transformed_df.filter(transformed_df.is_valid_customer == True).drop("is_valid_customer")
quarantine_records = transformed_df.filter(transformed_df.is_valid_customer == False)

# 5. Write to Silver Delta Tables
print("Writing to Silver & Quarantine tables...")
clean_records.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(config.silver_table)

if quarantine_records.count() > 0:
    quarantine_records.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(config.silver_quarantine_table)

print(f"Pipeline Execution Completed Successfully for {config.data_source}!")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS fmcg.silver.orders;
# MAGIC DROP TABLE IF EXISTS fmcg.silver.orders_quarantine;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS fmcg.gold.fact_orders;

# COMMAND ----------

import os

project_root = "/Workspace/Users/mahnoortauseef2022@gmail.com/fmcg_data_lakehouse"
tests_path = os.path.join(project_root, "tests")

print("Files in root project directory:", os.listdir(project_root))

if os.path.exists(tests_path):
    print("Files inside tests/ directory:", os.listdir(tests_path))
else:
    print("The tests/ folder does NOT exist at this path!")

# COMMAND ----------

import pytest
import sys
import os

# 1. Resolve project root
current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, "..")) if "src" not in os.listdir(current_dir) else current_dir

if project_root not in sys.path:
    sys.path.insert(0, project_root)

test_file = os.path.join(project_root, "tests", "tests_orders.py")

# 2. Run pytest disabling pycache & assertion rewriting on WSFS
retcode = pytest.main([
    test_file,
    "-v",
    "-s",
    "-o", "cache_dir=/tmp/.pytest_cache",     # Redirect pytest cache to driver temp storage
    "--assert=plain"                            # Disable bytecode assertion rewriting
])

if retcode != 0:
    raise RuntimeError(f"PyTest failed with exit code: {retcode}")