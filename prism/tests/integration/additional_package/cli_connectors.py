import os

from prism.connectors import SnowflakeConnector

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
