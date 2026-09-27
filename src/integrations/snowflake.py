"""Load Parquet exports from the configured Snowflake external stage."""

import os
import re


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_EXPORTS = {
    "orders": "orders",
    "customers": "customers",
    "products": "products",
    "pricing": "pricing",
}


def _identifier(value: str, name: str, parts: int = 1) -> str:
    components = value.removeprefix("@").split(".")
    if len(components) != parts or not all(_IDENTIFIER.fullmatch(part) for part in components):
        raise ValueError(f"{name} must contain {parts} valid SQL identifier(s)")
    return ".".join(components)


def load_staged_exports(connection, database: str, schema: str, stage: str) -> None:
    """COPY the exported Parquet folders into existing Snowflake tables."""
    database = _identifier(database, "database")
    schema = _identifier(schema, "schema")
    stage_parts = len(stage.removeprefix("@").split("."))
    stage = _identifier(stage, "stage", parts=stage_parts)
    cursor = connection.cursor()
    try:
        for table, folder in _EXPORTS.items():
            cursor.execute(
                f"COPY INTO {database}.{schema}.{table} "
                f"FROM @{stage}/{folder}/ "
                "FILE_FORMAT = (TYPE = PARQUET) "
                "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
                "PATTERN = '.*\\.parquet'"
            )
    finally:
        cursor.close()


def load_from_environment() -> None:
    """Connect with SNOWFLAKE_* environment variables and load staged exports."""
    required = ("ACCOUNT", "USER", "PASSWORD", "WAREHOUSE", "DATABASE", "SCHEMA", "STAGE")
    settings = {key: os.environ.get(f"SNOWFLAKE_{key}") for key in required}
    missing = [key for key, value in settings.items() if not value]
    if missing:
        names = ", ".join(f"SNOWFLAKE_{key}" for key in missing)
        raise RuntimeError(f"Missing Snowflake settings: {names}")

    from snowflake.connector import connect

    connection_options = {
        "account": settings["ACCOUNT"],
        "user": settings["USER"],
        "password": settings["PASSWORD"],
        "warehouse": settings["WAREHOUSE"],
        "database": settings["DATABASE"],
        "schema": settings["SCHEMA"],
    }
    role = os.environ.get("SNOWFLAKE_ROLE")
    if role:
        connection_options["role"] = role

    connection = connect(**connection_options)
    try:
        load_staged_exports(
            connection,
            settings["DATABASE"],
            settings["SCHEMA"],
            settings["STAGE"],
        )
    finally:
        connection.close()