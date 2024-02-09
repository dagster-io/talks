""" Demonstrate Assets & Configurations with NY Air Quality Reporting.

USAGE

    dagster dev -f project_blitz/demo_ny_air_quality.py

"""

from dagster import (
    AssetSelection,
    Config,
    ConfigurableResource,
    Definitions,
    EnvVar,
    MaterializeResult,
    MetadataValue,
    RunConfig,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from dagster_duckdb import DuckDBResource
from pandas.io.feather_format import pd
from pydantic import Field


########################################################################################
#                                      Resources                                       #
########################################################################################

duckdb_resource = DuckDBResource(database=EnvVar("DUCKDB_DATABASE"))


class CSVResource(ConfigurableResource):
    location: str = Field(description=("Path to CSV (file:// or https://)"))

    def load_dataset(self) -> pd.DataFrame:
        return pd.read_csv(self.location)


# City of New York - Air Quality - https://catalog.data.gov/dataset/air-quality
air_quality_resource = CSVResource(location=EnvVar("CSV_NY_AIR_QUALITY_LOCATION"))


########################################################################################
#                                        Assets                                        #
########################################################################################


@asset(group_name="reporting")
def ny_air_quality(database: DuckDBResource, air_quality_csv: CSVResource):
    df = air_quality_csv.load_dataset()

    # normalize column names
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    with database.get_connection() as conn:
        conn.cursor().execute(
            """
            CREATE OR REPLACE TABLE ny_air_quality
            AS
            SELECT * FROM df
            """
        )
    return MaterializeResult(
        metadata={
            "rows": MetadataValue.int(df.shape[0]),
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


@asset(deps=[ny_air_quality], group_name="reporting")
def ny_air_quality_report(database: DuckDBResource, config: ReportConfig):
    with database.get_connection() as conn:
        conn.cursor().execute(
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

########################################################################################
#                                         Jobs                                         #
########################################################################################

air_quality_report_job = define_asset_job(
    "my_asset_job",
    AssetSelection.groups("demo_assets"),
)

########################################################################################
#                                      Schedules                                       #
########################################################################################

my_job_hourly = ScheduleDefinition(
    name="custom_hourly_job",
    job=air_quality_report_job,
    cron_schedule="0 * * * *",  # Every hour
    run_config=RunConfig(ops={"ny_air_quality_report": ReportConfig()}),
)

my_job_daily = ScheduleDefinition(
    name="custom_daily_job",
    job=air_quality_report_job,
    cron_schedule="0 0 * * *",  # At 12:00 AM UTC
    run_config=RunConfig(
        ops={
            "ny_air_quality_report": ReportConfig(
                limit=1000, destination_table="ny_annual_average_report_1000"
            )
        }
    ),
)


########################################################################################
#                                     Definitions                                      #
########################################################################################

defs = Definitions(
    assets=[ny_air_quality, ny_air_quality_report],
    jobs=[air_quality_report_job],
    schedules=[my_job_hourly, my_job_daily],
    resources={
        "database": duckdb_resource,
        "air_quality_csv": air_quality_resource,
    },
)
