from dagster import ScheduleDefinition
from .jobs import bird_feeder_notebook_job

bird_feeder_schedule = ScheduleDefinition(
    job=bird_feeder_notebook_job,
    cron_schedule="0 0 * * *",  # daily at midnight
)
