import os
import time
import zipfile
from tempfile import NamedTemporaryFile
from typing import List, Tuple

import requests
import subprocess
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    OpExecutionContext,
    StaticPartitionsDefinition,
    asset,
    file_relative_path,
)
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_duckdb import DuckDBResource

from .constants import (
    CORNELL_BIRDWATCH_CHECKLISTS,
    SITE_DATA_FPATH,
    SITE_DESCRIPTION_DATA,
    SPECIES_TRANSLATION_DATA,
    SPECIES_TRANSLATION_FPATH,
)
from .resources import CustomDagsterDbtTranslator, dbt_manifest_path


def download_and_extract_data(context: AssetExecutionContext, url: str) -> Tuple[List[str], float]:
    with NamedTemporaryFile(suffix=".zip") as f:
        start_time = time.time()
        context.log.info("Downloading checklist data from {}".format(url))
        r = requests.get(url)
        context.log.info("Downloaded {} bytes".format(len(r.content)))
        f.write(r.content)
        f.seek(0)

        with zipfile.ZipFile(f.name, "r") as zip_ref:
            extracted_names = zip_ref.namelist()
            zip_ref.extractall(file_relative_path(__file__, "../data/raw/checklist_data"))
            end_time = time.time()
            context.log.info(
                "Extracted checklist data to {}".format(
                    file_relative_path(__file__, "../raw/checklist_data")
                )
            )

        return extracted_names, end_time - start_time


CHECKLIST_STATIC_PARTITIONS_DEF = StaticPartitionsDefinition(CORNELL_BIRDWATCH_CHECKLISTS)


@asset(
    partitions_def=CHECKLIST_STATIC_PARTITIONS_DEF,
    compute_kind="python",
    group_name="raw",
)
def cornell_feederwatch_checklists_raw(context: AssetExecutionContext) -> MaterializeResult:
    """The Cornell Lab of Ornithology and Birds Canada bird observations."""
    checklist = context.partition_key

    # skip if the file is already present on the file system
    destination_file_basename = os.path.splitext(os.path.basename(checklist))[0]
    destination_file = f"./data/raw/checklist_data/{destination_file_basename}.csv"

    if os.path.exists(destination_file):
        context.log.info("Skipping download of %s as file already exists", checklist)
        return MaterializeResult()

    extracted_names, elapsed_times = download_and_extract_data(context, checklist)
    return MaterializeResult(
        metadata={
            "names": extracted_names,
            "num_files": len(extracted_names),
            "elapsed_time": elapsed_times,
        },
    )


@asset(compute_kind="python", group_name="raw")
def cornell_site_descriptions_raw(context: AssetExecutionContext):
    """Supplementary information about the count locations (sites)."""
    extracted_names, elapsed_times = download_and_extract_data(context, SITE_DESCRIPTION_DATA)
    context.add_output_metadata(
        metadata={
            "names": extracted_names,
            "num_files": len(extracted_names),
            "elapsed_time": elapsed_times,
        },
    )


@asset(compute_kind="python", group_name="raw")
def cornell_species_translations_raw(context: AssetExecutionContext):
    """Species translation table stored in the Cornell Lab of Ornithology database."""
    extracted_names, elapsed_times = download_and_extract_data(context, SPECIES_TRANSLATION_DATA)
    context.add_output_metadata(
        metadata={
            "names": extracted_names,
            "num_files": len(extracted_names),
            "elapsed_time": elapsed_times,
        },
    )


@asset(
    deps=[cornell_feederwatch_checklists_raw],
    group_name="prepared",
    compute_kind="duckdb",
)
def birds(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    """Union of all bird observations data."""

    # We are extracting the ZIP files and loading them with `read_csv_auto` as DuckDB does not
    # currently support decompressing ZIP files directly. If this were the case, we might be able to
    # point directly to the hosted ZIP files on Cornell's website and drastically simplify this
    # pipeline.


    with duckdb.get_connection() as conn:
        # If the `birds` table already exists, and has a decent chunk of rows, we will skip
        # re-loading the CSV files. This is primarily to expedite the demo process, in production
        # you may want to consider alternatives.

        metadata = conn.execute("select * from duckdb_tables() where table_name = 'birds'").pl()
        if len(metadata["table_name"]) >= 1:
            nrows = conn.execute("select count(*) from birds").fetchone()[0]  # type: ignore
            if nrows > 40_000_000:
                context.log.info("Table has already been loaded; skipping...")
                return MaterializeResult(metadata={"num_rows": nrows})


        # construct `union` statement of CSV files for load into `birds` table
        checklist_file_paths = [
            f"./data/raw/checklist_data/{os.path.splitext(os.path.basename(checklist_url))[0]}.csv"
            for checklist_url in CORNELL_BIRDWATCH_CHECKLISTS
        ]

        sql_csv_select_statements = [
            f"select * from read_csv_auto('{path}', sample_size=-1)" for path in checklist_file_paths
        ]

        sql_csv_union_query = " UNION ALL ".join(sql_csv_select_statements)

        conn.execute(f"create or replace table birds as ({sql_csv_union_query})")
        nrows = conn.execute("select count(*) from birds").fetchone()[0]  # type: ignore
        conn.execute(f"create or replace table birds as ({sql_csv_union_query})")
        nrows = conn.execute("select count(*) from birds").fetchone()[0]  # type: ignore
        metadata = conn.execute("select * from duckdb_tables() where table_name = 'birds'").pl()

    return MaterializeResult(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "database_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )


@asset(
    deps=[cornell_species_translations_raw],
    compute_kind="duckdb",
    group_name="prepared",
)
def species(context: AssetExecutionContext, duckdb: DuckDBResource):
    species_csv_path = file_relative_path(__file__, SPECIES_TRANSLATION_FPATH)
    context.log.info("Loading species file: %s", species_csv_path)
    with duckdb.get_connection() as conn:
        conn.execute(f"""
            create or replace table species as (
                select * from read_csv_auto('{species_csv_path}')
            )
        """)

        nrows = conn.execute("""
            select count(*) from species
        """).fetchone()

        metadata = conn.execute("""
            select * from duckdb_tables() where table_name = 'species'
        """).pl()

    return MaterializeResult(
        metadata={
            "num_rows": nrows[0] if nrows else 0,
            "table_name": metadata["table_name"][0],
            "database_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )


@asset(
    deps=[cornell_site_descriptions_raw],
    compute_kind="duckdb",
    group_name="prepared",
)
def sites(context: AssetExecutionContext, duckdb: DuckDBResource):
    sites_csv_path = file_relative_path(__file__, SITE_DATA_FPATH)
    context.log.info(sites_csv_path)
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            create or replace table sites as (
                select * from read_csv_auto('{sites_csv_path}')
            )
            """
        )

        nrows = conn.execute("select count(*) from sites").fetchone()[0]  # type: ignore

        metadata = conn.execute("select * from duckdb_tables() where table_name = 'sites'").pl()

    return MaterializeResult(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "database_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )


@dbt_assets(manifest=dbt_manifest_path, dagster_dbt_translator=CustomDagsterDbtTranslator())
def dbt_birds(context: OpExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@asset(compute_kind="evidence", group_name="reporting", deps=[dbt_birds])
def evidence_dashboard():
    """Dashboard built using Evidence showing Duck metrics."""
    evidence_project_path = file_relative_path(__file__, "../reports")
    subprocess.run(["npm", "--prefix", evidence_project_path, "install"])
    subprocess.run(["npm", "--prefix", evidence_project_path, "run", "sources"])
    subprocess.run(["npm", "--prefix", evidence_project_path, "run", "build"])
