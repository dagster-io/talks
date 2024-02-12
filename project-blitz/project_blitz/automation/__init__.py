from dagster import Definitions, load_assets_from_modules
from dagstermill import ConfigurableLocalOutputNotebookIOManager
from . import assets, resources, jobs, schedules, sensors


asset_defs = load_assets_from_modules([assets])


defs = Definitions(
    assets=asset_defs,
    resources={
        "duckdb": resources.duckdb_resource,
        "output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager(),
    },
    jobs=[
        jobs.bird_feeder_notebook_job,
    ],
    schedules=[schedules.bird_feeder_schedule],
    sensors=[sensors.bird_observation],
)
