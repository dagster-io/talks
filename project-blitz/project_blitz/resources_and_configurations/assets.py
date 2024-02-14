from dagster import (
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)

from dagster_duckdb import DuckDBResource
from .resources import CSVResource


@asset(
    group_name="ny_air_quality",
    compute_kind="DuckDB",
)
def ny_air_quality(
    database: DuckDBResource, air_quality_csv: CSVResource
) -> MaterializeResult:
    """New York state Air Quality metrics."""
    df = air_quality_csv.load_dataset()

    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    with database.get_connection() as conn:
        conn.execute(
            """
            CREATE OR REPLACE TABLE ny_air_quality
            AS
            SELECT * FROM df
            """
        )

        metadata = conn.execute(
            """
            SELECT
              table_name,
              database_name,
              schema_name,
              column_count,
              estimated_size
            FROM duckdb_tables()
            WHERE table_name = 'ny_air_quality'
            """
        ).fetchall()

    return MaterializeResult(
        metadata={
            "num_rows": MetadataValue.int(df.shape[0]),
            "table_name": metadata[0][0],
            "database_name": metadata[0][1],
            "schema_name": metadata[0][2],
            "column_count": metadata[0][3],
            "estimated_size": metadata[0][4],
        }
    )

# ┌────────────────┬─────────────┬─────────┬─────────┬─────────┬───────┐
# │  column_name   │ column_type │  null   │   key   │ default │ extra │
# │    varchar     │   varchar   │ varchar │ varchar │ varchar │ int32 │
# ├────────────────┼─────────────┼─────────┼─────────┼─────────┼───────┤
# │ unique_id      │ BIGINT      │ YES     │         │         │       │
# │ indicator_id   │ BIGINT      │ YES     │         │         │       │
# │ name           │ VARCHAR     │ YES     │         │         │       │
# │ measure        │ VARCHAR     │ YES     │         │         │       │
# │ measure_info   │ VARCHAR     │ YES     │         │         │       │
# │ geo_type_name  │ VARCHAR     │ YES     │         │         │       │
# │ geo_join_id    │ BIGINT      │ YES     │         │         │       │
# │ geo_place_name │ VARCHAR     │ YES     │         │         │       │
# │ time_period    │ VARCHAR     │ YES     │         │         │       │
# │ start_date     │ VARCHAR     │ YES     │         │         │       │
# │ data_value     │ DOUBLE      │ YES     │         │         │       │
# │ message        │ DOUBLE      │ YES     │         │         │       │
# ├────────────────┴─────────────┴─────────┴─────────┴─────────┴───────┤
# │ 12 rows                                                  6 columns │
# └────────────────────────────────────────────────────────────────────┘


# Demonstrate the use of `Config` with an Asset

class ReportConfig(Config):
    limit: int = 10
    measure_type: str = "Nitrogen dioxide (NO2)"
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
            FROM ny_air_quality
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

        metadata = conn.execute(
            """
            SELECT
              table_name,
              database_name,
              schema_name,
              column_count,
              estimated_size
            FROM duckdb_tables()
            WHERE table_name = 'ny_air_quality'
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

# ┌────────────────────────┬──────────────────────────────────────┬──────────────┬────────────┐
# │          name          │            geo_place_name            │ measure_info │ mean_value │
# │        varchar         │               varchar                │   varchar    │   double   │
# ├────────────────────────┼──────────────────────────────────────┼──────────────┼────────────┤
# │ Nitrogen dioxide (NO2) │ Midtown (CD5)                        │ ppb          │      34.93 │
# │ Nitrogen dioxide (NO2) │ Gramercy Park - Murray Hill          │ ppb          │      32.63 │
# │ Nitrogen dioxide (NO2) │ Chelsea - Clinton                    │ ppb          │      30.66 │
# │ Nitrogen dioxide (NO2) │ Stuyvesant Town and Turtle Bay (CD6) │ ppb          │      30.36 │
# │ Nitrogen dioxide (NO2) │ Chelsea-Village                      │ ppb          │      29.53 │
# │ Nitrogen dioxide (NO2) │ Upper East Side-Gramercy             │ ppb          │      29.39 │
# │ Nitrogen dioxide (NO2) │ Clinton and Chelsea (CD4)            │ ppb          │      28.42 │
# │ Nitrogen dioxide (NO2) │ Financial District (CD1)             │ ppb          │      28.13 │
# │ Nitrogen dioxide (NO2) │ Lower Manhattan                      │ ppb          │      28.06 │
# │ Nitrogen dioxide (NO2) │ Greenwich Village and Soho (CD2)     │ ppb          │      27.31 │
# ├────────────────────────┴──────────────────────────────────────┴──────────────┴────────────┤
# │ 10 rows                                                                         4 columns │
# └───────────────────────────────────────────────────────────────────────────────────────────┘
