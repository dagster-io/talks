from dagster import (
    AssetSelection,
    define_asset_job,
)


air_quality_report_job = define_asset_job(
    "air_quality_report",
    AssetSelection.groups("reporting"),
)
