from dagster import define_asset_job

bird_feeders_table_job = define_asset_job(
    name="bird_feeders_table_job", selection=["bird_feeders_table", "bird_codes"]
)

bird_feeder_notebook_job = define_asset_job(
    name="bird_feeder_job", selection="bird_notebook"
)
