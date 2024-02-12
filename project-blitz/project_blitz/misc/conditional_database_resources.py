""" Demonstrate DuckDB / Snowflake Conditional Usage.
"""


import os
import pandas as pd


from dagster import (
    Config,
    Definitions,
    EnvVar,
    MaterializeResult,
    MetadataValue,
    ResourceParam,
    asset,
)
from dagster_duckdb import DuckDBResource
from dagster_snowflake import SnowflakeResource
from typing import Union


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


class CerealConfig(Config):
    cereal_csv_path: str = "https://docs.dagster.io/assets/cereal.csv"


@asset(
    compute_kind="DuckDB",
    group_name="cereal",
)
def cereal(
    database: ResourceParam[Union[SnowflakeResource, DuckDBResource]],
    config: CerealConfig,
) -> MaterializeResult:
    """Nutrition information for popular cereal brands."""
    df = pd.read_csv(config.cereal_csv_path)
    with database.get_connection() as conn:
        conn.cursor().execute(
            """
            CREATE OR REPLACE TABLE cereal
            AS
            SELECT * FROM df
            """
        )
    return MaterializeResult(
        metadata={
            "rows": MetadataValue.int(df.shape[0]),
        }
    )


class HealthyCerealConfig(Config):
    table_name: str = "healthy_cereal"
    minimum_protein: int = 4
    maximum_sugars: int = 6


@asset(
    deps=[cereal],
    compute_kind="DuckDB",
    group_name="cereal",
)
def healthy_cereal(
    database: ResourceParam[Union[SnowflakeResource, DuckDBResource]],
    config: HealthyCerealConfig,
) -> None:
    """Nutrition information for _healthy_ cereal brands."""
    with database.get_connection() as conn:
        conn.cursor().execute(
            f"""
            CREATE OR REPLACE TABLE {config.table_name}
            AS
            SELECT * FROM foods.cereal
            WHERE
                (1 / cups) * protein >= {config.minimum_protein}
                AND (1 / cups) * sugars <= {config.maximum_sugars}
            ORDER BY fiber DESC
            """
        )


defs = Definitions(
    assets=[cereal, healthy_cereal],
    resources=resources[os.getenv("DAGSTER_ENV", "local")],
)
