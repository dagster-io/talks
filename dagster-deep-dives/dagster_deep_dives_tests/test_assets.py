from dagster import asset, ConfigurableResource


class DuckDBResource(ConfigurableResource):
    path: str


@asset
def asset_requires_bar(bar: DuckDBResource) -> str:
    return bar.path


def test_asset_requires_bar():
    result = asset_requires_bar(bar=DuckDBResource(path="test.duckdb"))
    assert result == "test.duckdb"
