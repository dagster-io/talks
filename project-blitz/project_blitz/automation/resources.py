from dagster import EnvVar
from dagster_duckdb import DuckDBResource

duckdb_resource = DuckDBResource(database=EnvVar("DUCKDB_DATABASE"))
