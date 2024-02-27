from setuptools import find_packages, setup

setup(
    name="project_blitz",
    packages=find_packages(exclude=["project_blitz_tests"]),
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
