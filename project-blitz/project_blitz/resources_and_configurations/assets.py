from dagster import (
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)

from dagster_duckdb import DuckDBResource
from .resources import CSVResource


class NYAirQualityConfig(Config):
    destination_table: str = "ny_air_quality"


@asset(
    group_name="ny_air_quality",
    compute_kind="DuckDB",
)
def ny_air_quality(
    database: DuckDBResource, air_quality_csv: CSVResource, config: NYAirQualityConfig
) -> MaterializeResult:
    """New York state Air Quality metrics."""
    df = air_quality_csv.load_dataset()

    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    with database.get_connection() as conn:
        conn.execute(
            # NOTE: beware of SQL injection; unable to find support for the psycopg2
            # equivalent of sql.Identifiers for DuckDB
            f"""
            CREATE OR REPLACE TABLE {config.destination_table}
            AS
            SELECT * FROM df
            """
        )

        metadata = conn.execute(
            f"""
            SELECT
              table_name,
              database_name,
              schema_name,
              column_count,
              estimated_size
            FROM duckdb_tables()
            WHERE table_name = '{config.destination_table}'
            """
        ).fetchall()

    return MaterializeResult(
        metadata={
            "table_name": metadata[0][0],
            "database_name": metadata[0][1],
            "schema_name": metadata[0][2],
            "column_count": metadata[0][3],
            "estimated_size": metadata[0][4],
        }
    )


class ReportConfig(Config):
    limit: int = 10
    measure_type: str = "Nitrogen dioxide (NO2)"
    source_table: str = "ny_air_quality"
    destination_table: str = "ny_annual_average_report"


@asset(
    deps=[ny_air_quality],
    group_name="ny_air_quality",
    compute_kind="DuckDB",
)
def ny_air_quality_report(database: DuckDBResource, config: ReportConfig):
    """Top offendors for specific air quality metric."""
    with database.get_connection() as conn:
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE {config.destination_table} AS
            SELECT
              name,
              geo_place_name,
              measure_info,
              round(mean(data_value), 2) as mean_value
            FROM {config.source_table}
            WHERE
              time_period like 'Annual Average %'
              AND name = '{config.measure_type}'
            GROUP BY
              name,
              geo_place_name,
              measure_info
            ORDER BY mean_value DESC
            LIMIT {config.limit}
            """
        )

        results = conn.execute(
            f"""
            SELECT * FROM {config.destination_table}
            """
        ).df()

        metadata = conn.execute(
            f"""
            SELECT
              table_name,
              database_name,
              schema_name,
              column_count,
              estimated_size
            FROM duckdb_tables()
            WHERE table_name = '{config.destination_table}'
            """
        ).fetchall()

    return MaterializeResult(
        metadata={
            "table_name": metadata[0][0],
            "database_name": metadata[0][1],
            "schema_name": metadata[0][2],
            "column_count": metadata[0][3],
            "estimated_size": metadata[0][4],
            "preview": MetadataValue.md(str(results.head(5).to_markdown())),
        }
    )
