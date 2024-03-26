import os

from prism.connectors import (
    PostgresConnector,
    SnowflakeConnector,
)


postgres_connector = PostgresConnector(
    id="postgres-connector",
    user=os.environ.get("POSTGRES_USER"),
    password=os.environ.get("POSTGRES_PASSWORD"),
    port=5432,
    host=os.environ.get("POSTGRES_HOST"),
    database=os.environ.get("POSTGRES_DB"),
    autocommit=True,
)


snowflake_connector = SnowflakeConnector(
    id="snowflake-connector",
    user=os.environ.get("SNOWFLAKE_USER"),
    password=os.environ.get("SNOWFLAKE_PASSWORD"),
    account=os.environ.get("SNOWFLAKE_ACCOUNT"),
    role=os.environ.get("SNOWFLAKE_ROLE"),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
    database=os.environ.get("SNOWFLAKE_DATABASE"),
    schema=os.environ.get("SNOWFLAKE_SCHEMA"),
)
