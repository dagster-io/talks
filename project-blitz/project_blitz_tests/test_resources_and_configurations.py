import pytest
import pandas as pd

from dagster import MaterializeResult, IntMetadataValue
from dagster_duckdb import DuckDBResource
from project_blitz.resources_and_configurations.resources import (
    CSVResource,
)
from project_blitz.resources_and_configurations.assets import (
    ReportConfig,
    ny_air_quality,
    ny_air_quality_report,
)


def test_csv_resource_to_dataframe():
    csv_resource = CSVResource(
        location="./project_blitz_tests/ny_air_quality_sample.csv"
    )
    df = csv_resource.load_dataset()
    assert type(df) == pd.DataFrame
    assert df.shape == (5, 12)


# Note:
#
# The `tmpdir_factory` fixture is used to store the DuckDB database at a temporary
# location in the scope of each session. For more information, see:
#
# https://docs.pytest.org/en/6.2.x/tmpdir.html#the-tmpdir-factory-fixture


@pytest.fixture(scope="session")
def database_path(tmpdir_factory):
    return str(tmpdir_factory.mktemp("data").join("data.duckdb"))


def test_asset_ny_air_quality(database_path):
    duckdb_resource = DuckDBResource(database=database_path)
    csv_resource = CSVResource(
        location="./project_blitz_tests/ny_air_quality_sample.csv"
    )

    # ensure metadata is correctly stored on the `MaterializeResult`
    materialize_result = ny_air_quality(duckdb_resource, csv_resource)
    assert materialize_result == MaterializeResult(
        asset_key=None,
        metadata={
            "num_rows": IntMetadataValue(value=5),
            "table_name": "ny_air_quality",
            "database_name": "data",
            "schema_name": "main",
            "column_count": 12,
            "estimated_size": 5,
        },
        check_results=[],
        data_version=None,
    )

    # validate content of the `ny_air_quality` table
    with duckdb_resource.get_connection() as conn:
        results = conn.sql(
            """
            SELECT * FROM ny_air_quality
            """
        ).fetchall()
        assert len(results) == 5


def test_asset_ny_air_quality_resport(database_path):
    duckdb_resource = DuckDBResource(database=database_path)
    config = ReportConfig()
    ny_air_quality_report(duckdb_resource, config)

    # validate content of the `ny_air_quality_report` table
    with duckdb_resource.get_connection() as conn:
        results = conn.sql(
            f"""
            SELECT * FROM {config.destination_table}
            """
        ).fetchall()
        assert len(results) == 5
