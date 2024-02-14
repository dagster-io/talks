from dagster import ScheduleDefinition, RunConfig

from .jobs import air_quality_report_job
from .assets import ReportConfig


report_hourly = ScheduleDefinition(
    name="custom_hourly_job",
    job=air_quality_report_job,
    cron_schedule="0 * * * *",  # Every hour
)

report_daily = ScheduleDefinition(
    name="custom_daily_job",
    job=air_quality_report_job,
    cron_schedule="0 0 * * *",  # At 12:00 AM UTC
    run_config=RunConfig(
        ops={
            "ny_air_quality_report": ReportConfig(
                limit=1000, destination_table="ny_annual_average_report_1000"
            )
        }
    ).to_config_dict(),
)
