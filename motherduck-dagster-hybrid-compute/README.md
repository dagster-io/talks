# Motherduck / Dagster Hybrid Compute

This demo project is built from the foundations of the open source MDS found at [dagster-io/mdsfest-opensource-mds](https://github.com/dagster-io/mdsfest-opensource-mds).

This stack is built on a combination of tools including:

- [Dagster](https://dagster.io)
- [DuckDB](https://duckdb.org)
- [dbt](https://www.getdbt.com)
- [dbt-duckdbt](https://github.com/jwills/dbt-duckdb)
- [Evidence](https://evidence.dev)

## Requirements

You will need Python installed. This was all tested on Python 3.10.12
From a virtual environment, run

```python
make install
```
Most of the dependencies will be installed through Python.

For Evidence.dev, you will need [nodejs](https://nodejs.org/en/download) installed

To run Dagster locally, issue the following command:

```shell
make dev
```

Navigate to http://localhost:3000/, and click Materialize all to run the end-to-end pipeline.

## Visualization

Evidence.dev is used for visualization.

First, go the `dbt_project` folder

```
cd dbt_project

npm --prefix ./reports install
npm --prefix ./reports run dev -- --port 4000
```

