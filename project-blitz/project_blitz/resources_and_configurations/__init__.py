""" Demonstrate Assets & Configurations with NY Air Quality Reporting.
"""

from dagster import (
    Definitions,
)

from .assets import ny_air_quality, ny_air_quality_report
from .jobs import air_quality_report_job
from .schedules import report_daily, report_hourly
from .resources import duckdb_resource, air_quality_resource


defs = Definitions(
    assets=[ny_air_quality, ny_air_quality_report],
    jobs=[air_quality_report_job],
    schedules=[report_hourly, report_daily],
    resources={
        "database": duckdb_resource,
        "air_quality_csv": air_quality_resource,
    },
)
