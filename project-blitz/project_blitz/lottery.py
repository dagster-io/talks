import pandas as pd


from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset


URL_NY_POWERBALL_WINNING_NUMBERS = "https://data.ny.gov/api/views/d6yy-54nr/rows.csv"


@asset(
    required_resource_keys={"database"},
    compute_kind="Python",
)
def ny_powerball_numbers(context: AssetExecutionContext) -> MaterializeResult:
    """New York Lottery Powerball Winning Numbers: Beginning 2010"""
    df = pd.read_csv(URL_NY_POWERBALL_WINNING_NUMBERS)
    with context.resources.database.get_connection() as conn:
        conn.cursor().execute(
            """
            CREATE OR REPLACE TABLE ny_powerball_numbers
            AS
            SELECT * FROM df
            """
        )
    return MaterializeResult(
        metadata={
            "rows": MetadataValue.int(df.shape[0]),
        }
    )


pd.read_json("https://catalog.data.gov/harvest/object/b572e7d6-78fe-466b-9c4f-7a8c7937839e")


df = pd.read_csv("https://data.ny.gov/api/views/ekci-x6aq/rows.csv")

df

# City of Chicago - Speed Camera Violations
url = "https://data.cityofchicago.org/api/views/hhkd-xvj4/rows.csv"

df = pd.read_csv(url)


class MyAssetConfig(Config):
    a_str: str

@asset
def my_asset(config: MyAssetConfig):
    assert config.a_str == "foo"

materialize(
    [my_asset],
    run_config=RunConfig(
        ops={"my_asset": MyAssetConfig(a_str="foo")}
    )
)
