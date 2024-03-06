# Dagster Deep Dives

## Usage

Install dependencies with: 

```shell
make install


Run a specific example with (note you must run from the `dagster-deep-dives` root directory for environment variables to take effect.

```shell
make run

Select the example project to run:
1) automation
2) resources-and-configuration
n) ...
#?
```

Or manually choose the project to run:


```shell
dagster dev -m dagster_deep_dives.automation

# or

dagster dev -m dagster_deep_dives.resources_and_configurations
```
