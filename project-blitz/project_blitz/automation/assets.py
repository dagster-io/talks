from dagster import (
    AutoMaterializePolicy,
    Config,
    MetadataValue,
    asset,
    AssetExecutionContext,
    file_relative_path,
)
from dagstermill import define_dagstermill_asset
from .resources import DuckDBResource


class BirdFeederConfig(Config):
    filename: str


@asset(compute_kind="duckdb", group_name="raw")
def bird_feeders_table(
    context: AssetExecutionContext, config: BirdFeederConfig, duckdb: DuckDBResource
):
    fpath = file_relative_path(__file__, "../data/bird_data/" + config.filename)
    with duckdb.get_connection() as conn:
        query = f"""
            CREATE OR REPLACE TABLE tmp_bird_feeder AS (
                     select 
                     *, 
                    '{fpath}' as filename,
                     try_cast(how_many as numeric) as n_obs 
                     from read_csv('{fpath}', sample_size=500000, auto_detect=true)
            )"""
        context.log.info(query)
        conn.execute(query)
        context.log.info("Created tmp_bird_feeder view")

        conn.execute(
            "CREATE TABLE IF NOT EXISTS bird_feeders AS FROM tmp_bird_feeder LIMIT 0"
        )
        conn.execute(f"DELETE FROM bird_feeders WHERE filename = '{fpath}'")
        context.log.info("Deleted old data for %s", fpath)

        conn.execute("INSERT INTO bird_feeders SELECT * FROM tmp_bird_feeder")
        context.log.info("Inserted new data for %s", fpath)

        conn.execute("DROP TABLE tmp_bird_feeder")

        context.log.info("Updated bird_feeders table for %s", fpath)

        nrows = conn.execute("SELECT COUNT(*) FROM bird_feeders").fetchone()[0]  # type: ignore
        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'bird_feeders'"
        ).pl()

        context.add_output_metadata(
            metadata={
                "num_rows": nrows,
                "table_name": metadata["table_name"][0],
                "datbase_name": metadata["database_name"][0],
                "schema_name": metadata["schema_name"][0],
                "column_count": metadata["column_count"][0],
                "estimated_size": metadata["estimated_size"][0],
            }
        )


@asset(compute_kind="duckdb", group_name="raw")
def bird_codes(context: AssetExecutionContext, duckdb: DuckDBResource):
    fpath = file_relative_path(
        __file__, "../data/bird_data/PFW_spp_translation_table_May2023.csv"
    )
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE bird_codes AS (
                select * from read_csv('{fpath}', auto_detect=true)
            )"""
        )
        context.log.info("Created bird_codes table")

        nrows = conn.execute("SELECT COUNT(*) FROM bird_codes").fetchone()[0]  # type: ignore
        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'bird_codes'"
        ).pl()

        context.add_output_metadata(
            metadata={
                "num_rows": nrows,
                "table_name": metadata["table_name"][0],
                "datbase_name": metadata["database_name"][0],
                "schema_name": metadata["schema_name"][0],
                "column_count": metadata["column_count"][0],
                "estimated_size": metadata["estimated_size"][0],
            }
        )


@asset(
    compute_kind="duckdb",
    deps=[bird_feeders_table, bird_codes],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def bird_aggregates(context: AssetExecutionContext, duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            CREATE OR REPLACE VIEW bird_aggregates AS (
                SELECT
                    year,
                    species_code,
                    american_english_name as species_name,
                    count(1) as total_observations,
                    sum(n_obs) as total_birds,
                    avg(n_obs) as avg_birds_per_observation,
                    approx_quantile(n_obs, 0.5) as median_birds_per_observation
                FROM bird_feeders
                JOIN bird_codes using(species_code)
                where n_obs is not null
                GROUP BY ALL
            )
            """
        )
        # Get the top 2 rows by year sorted by median

        bird_aggs = conn.execute(
            """
        select 
            year, 
            species_code, 
            species_name, 
            total_birds, 
            median_birds_per_observation
        from bird_aggregates
        qualify row_number() over (
            partition by year order by median_birds_per_observation desc) = 1
        order by year desc"""
        ).df()
        context.log.info("Created bird_aggregates view")
        preview = {"preview": MetadataValue.md(bird_aggs.head(3).to_markdown())}  # type: ignore
        context.add_output_metadata(metadata=preview)


bird_notebook = define_dagstermill_asset(
    name="bird_notebook",
    notebook_path=file_relative_path(__file__, "birdbook.ipynb"),
    deps=[bird_aggregates],
)
