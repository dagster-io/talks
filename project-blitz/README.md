# Feature Blitz

## Usage

Install dependencies with: 

```shell
make install


Run a specific example with (note you must run from the `project-blitz` root directory for environment variables to take effect.

```shell
make run

Select the example project to run:
1) automation
2) resources-and-configuration
#?
```

Or manually choose the project to run:


```shell
dagster dev -m project_blitz.automation

# or

dagster dev -m project_blitz.resources_and_configurations
```
