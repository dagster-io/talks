# Motherduck / Dagster Hybrid Compute

This demo project is built from the foundations of the open source MDS found at [dagster-io/mdsfest-opensource-mds](https://github.com/dagster-io/mdsfest-opensource-mds).

This stack is built on a combination of tools including:

- [Dagster](https://dagster.io)
- [DuckDB](https://duckdb.org)
- [dbt](https://www.getdbt.com)
- [dbt-duckdbt](https://github.com/jwills/dbt-duckdb)
- [Evidence](https://evidence.dev)

## Requirements

### Dagster

From a virtual environment, run

```bash
make install
```
For Evidence.dev, you will need [nodejs](https://nodejs.org/en/download) installed

> [!IMPORTANT]
> At the time of this writing, Evidence requires Node v20. We recommend using [nvm](https://github.com/nvm-sh/nvm) to manage Node versions (eg. `nvm use 20`).

Runing Dagster:

```bash
make dev
```

Navigate to http://localhost:3000/, and click Materialize all to run the end-to-end pipeline.

### Evidence

The pipeline will generate the Evidence dashboard HTML files. To preview the dashboard you can use the following directive:

```bash
make evidence-preview
```

Navigate to http://localhost:4000/ to view the report.
