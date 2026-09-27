"""Load Databricks Parquet exports from S3 into Snowflake."""

import logging

from src.integrations.snowflake import load_from_environment


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    load_from_environment()