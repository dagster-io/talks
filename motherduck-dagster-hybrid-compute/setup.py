from setuptools import setup

setup(
    name="motherduck_dagster_hybrid",
    install_requires=[
        "duckdb",
        "dagster",
        "dagster-webserver",
        "dagster-dbt",
        "dagster-duckdb",
        "duckdb",
        "polars",
        "pyarrow",
        "dbt-duckdb",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
