from setuptools import find_packages, setup

setup(
    name="motherduck_dagster_hybrid",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-dbt",
        "dagster-duckdb",
        "dagster-webserver",
        "dbt-duckdb",
        "duckdb==v0.9.2",
        "polars",
        "pyarrow",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "ruff"]},
)
