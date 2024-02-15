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


air_quality_resource = CSVResource(location=EnvVar("NY_AIR_QUALITY_CSV"))
