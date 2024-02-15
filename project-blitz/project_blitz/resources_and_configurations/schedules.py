from dagster import ScheduleDefinition, RunConfig

from .jobs import air_quality_report_job
from .assets import ReportConfig


report_hourly = ScheduleDefinition(
    name="ny_air_quality_small_hourly",
    job=air_quality_report_job,
    cron_schedule="0 * * * *",  # Every hour
)

report_daily = ScheduleDefinition(
    name="ny_air_quality_large_daily",
    job=air_quality_report_job,
    cron_schedule="0 0 * * *",  # At 12:00 AM UTC
    run_config=RunConfig(
        ops={
            "ny_air_quality_report": ReportConfig(
                limit=100,
                destination_table="ny_annual_average_report_100",
                measure_type="Fine particles (PM 2.5)",
            )
        }
    ).to_config_dict(),
)
