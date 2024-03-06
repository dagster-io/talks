from setuptools import find_packages, setup

setup(
    name="dagster_deep_dives",
    packages=find_packages(exclude=["dagster_deep_dives_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-duckdb",
        "dagster-snowflake",
        "dagstermill",
        "pandas",
        "seaborn",
        "matplotlib",
        "Faker",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "ruff"]},
)
