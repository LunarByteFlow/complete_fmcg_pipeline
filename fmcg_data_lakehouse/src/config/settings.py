"""
settings.py
-----------
Centralized Configuration Module for the FMCG Data Lakehouse.
Provides dynamic path resolution, table namespace management, 
and workspace environment variables.
"""

from typing import Optional
from pyspark.sql import SparkSession


class PipelineConfig:
    """
    Manages operational parameters, table metadata, and S3 storage URIs 
    for the FMCG Data Pipeline.
    
    Attributes:
        spark (SparkSession): Active PySpark session instance.
        catalog (str): Target Unity Catalog name (e.g., 'fmcg').
        data_source (str): Specific domain dataset being processed (e.g., 'orders', 'customers').
        bronze_schema (str): Schema name for raw ingestion layer.
        silver_schema (str): Schema name for cleaned & validated layer.
        gold_schema (str): Schema name for aggregated business entity layer.
        bronze_table (str): Fully qualified Delta table name for Bronze layer.
        silver_table (str): Fully qualified Delta table name for Silver layer.
        silver_quarantine_table (str): Fully qualified Delta table for quarantined records.
        gold_table (str): Fully qualified Delta table for domain Gold facts/dimensions.
        gold_parent_table (str): Fully qualified Delta table for enterprise parent metrics.
        base_path (str): Root AWS S3 URI for the current dataset domain.
        landing_path (str): S3 ingress path where raw files arrive.
    """

    def __init__(
        self, 
        spark_session: SparkSession, 
        catalog: str = "fmcg", 
        data_source: str = "orders",
        environment: str = "prod"
    ) -> None:
        """
        Initializes pipeline configuration with dynamic namespace resolution.

        Args:
            spark_session (SparkSession): Current active PySpark session.
            catalog (str, optional): Target database catalog. Defaults to "fmcg".
            data_source (str, optional): Target pipeline domain dataset. Defaults to "orders".
            environment (str, optional): Deployment tier ('dev', 'qa', 'prod'). Defaults to "prod".
        """
        self.spark: SparkSession = spark_session
        self.catalog: str = catalog
        self.data_source: str = data_source
        self.environment: str = environment

        # Medallion Architecture Schema Namespaces
        self.bronze_schema: str = "bronze"
        self.silver_schema: str = "silver"
        self.gold_schema: str = "gold"

        # Fully Qualified Delta Table Identifiers (catalog.schema.table)
        self.bronze_table: str = f"{self.catalog}.{self.bronze_schema}.{self.data_source}"
        self.silver_table: str = f"{self.catalog}.{self.silver_schema}.{self.data_source}"
        self.silver_quarantine_table: str = (
            f"{self.catalog}.{self.silver_schema}.{self.data_source}_quarantine"
        )
        self.gold_table: str = f"{self.catalog}.{self.gold_schema}.sb_fact_{self.data_source}"
        self.gold_parent_table: str = f"{self.catalog}.{self.gold_schema}.fact_orders"

        # Object Storage Cloud Paths (AWS S3)
        self.base_path: str = f"s3://sportsbar-db-484395054876-us-east-1-an/{self.data_source}"
        self.landing_path: str = f"{self.base_path}/landing/"
        self.processed_path: str = f"{self.base_path}/processed/"
        self.checkpoint_path: str = f"{self.base_path}/_checkpoints/{self.data_source}"

    def __repr__(self) -> str:
        return (
            f"<PipelineConfig env='{self.environment}' "
            f"catalog='{self.catalog}' "
            f"source='{self.data_source}'>"
        )