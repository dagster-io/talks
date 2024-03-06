from dagster import define_asset_job


air_quality_report_job = define_asset_job(
    "air_quality_report",
    ["ny_air_quality", "ny_air_quality_report"],
)
