from dagster import (
    ConfigurableResource,
    EnvVar,
)

from dagster_duckdb import DuckDBResource
from pandas.io.feather_format import pd
from pydantic import Field

duckdb_resource = DuckDBResource(database=EnvVar("DUCKDB_DATABASE"))


class CSVResource(ConfigurableResource):
    location: str = Field(description=("Path to CSV (file:// or https://)"))

    def load_dataset(self) -> pd.DataFrame:
        return pd.read_csv(self.location)


env = str(EnvVar("ENVIRONMENT").get_value())

NY_AQ_SAMPLE = "data/ny-air-quality-sample.csv"
NY_AQ = "https://data.cityofnewyork.us/api/views/c3uy-2p5r/rows.csv"

csv_locations = {
    "local": NY_AQ_SAMPLE,
    "staging": NY_AQ,
    "production": NY_AQ,
    "branch": NY_AQ,
}

air_quality_resource = CSVResource(location=csv_locations.get(env, NY_AQ))
