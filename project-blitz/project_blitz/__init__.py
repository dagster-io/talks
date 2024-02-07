import os
import pandas as pd


from dagster import (
    AssetExecutionContext,
    Definitions,
    EnvVar,
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster_duckdb import DuckDBResource
from dagster_snowflake import SnowflakeResource


snowflake_resource = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
    database=EnvVar("SNOWFLAKE_DATABASE"),
    schema=EnvVar("SNOWFLAKE_SCHEMA"),
)


duckdb_resource = DuckDBResource(database=EnvVar("DUCKDB_DATABASE"))

resources = {
    "local": {"database": duckdb_resource},
    "testing": {"database": duckdb_resource},
    "production": {"database": snowflake_resource},
    "staging": {"database": snowflake_resource},
}


@asset(
    required_resource_keys={"database"},
    compute_kind="DuckDB",
)
def foods_schema(context: AssetExecutionContext) -> None:
    """Foods schema in target database."""
    with context.resources.database.get_connection() as conn:
        conn.cursor().execute(
            """
            CREATE SCHEMA IF NOT EXISTS foods
            """
        )


@asset(
    deps=[foods_schema],
    required_resource_keys={"database"},
    compute_kind="DuckDB",
)
def cereal(context: AssetExecutionContext) -> MaterializeResult:
    """Nutrition information for popular cereal brands."""
    # TODO - parameterize the CSV file
    df = pd.read_csv("https://docs.dagster.io/assets/cereal.csv")
    with context.resources.database.get_connection() as conn:
        conn.cursor().execute(
            """
            CREATE OR REPLACE TABLE foods.cereal
            AS
            SELECT * FROM df
            """
        )
    return MaterializeResult(
        metadata={
            "rows": MetadataValue.int(df.shape[0]),
        }
    )


@asset(
    deps=[cereal],
    required_resource_keys={"database"},
    compute_kind="DuckDB",
)
def healthy_cereal(context: AssetExecutionContext) -> MaterializeResult:
    """Nutrition information for _healthy_ cereal brands."""
    with context.resources.database.get_connection() as conn:
        conn.cursor().execute(
            """
            CREATE OR REPLACE TABLE foods.healthy_cereal
            AS
            SELECT * FROM foods.cereal
            WHERE
                (1 / cups) * protein >= 4
                AND (1 / cups) * sugars <= 4
            ORDER BY fiber DESC
            """
        )


defs = Definitions(
    assets=[foods_schema, cereal, healthy_cereal],
    resources=resources[os.getenv("DAGSTER_ENV", "local")],
)


# References
#
# - https://docs.dagster.io/concepts/configuration/config-schema#defining-and-accessing-pythonic-configuration-for-a-resource
# - https://docs.dagster.io/deployment/dagster-instance#default-local-behavior
# - https://docs.dagster.io/deployment/dagster-instance#configuration-reference
# - https://docs.dagster.io/concepts/resources
