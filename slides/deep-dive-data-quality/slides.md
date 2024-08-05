---
theme: default
title: 'Dagster Deep Dive: Data Quality'
drawings:
  persist: false
transition: slide-left
themeConfig:
    primary: '#4F43DD'
background: '/images/background.png'
favicon: '/images/dagsir.png'
---

<img src="/images/dagster-reversed-horizontal.svg" class="h-12 abs-tr mt-12 mr-12 z-99" />

# Dagster Deep Dive

## Data Quality: Building Reliable Data Platforms

Colton Padden - Developer Advocate

<div class="flex space-x-4">
  <a href="https://github.com/cmpadden" target="_blank"><ri-github-line /></a>
  <a href="https://twitter.com/coltonpadden" target="_blank"><ri-twitter-line /></a>
  <a href="https://linkedin.com/colton-padden" target="_blank"><ri-linkedin-line /></a>
</div>

---
layout: center
---

# Agenda

- What is it?
- Why is it important?
- 6 dimensions of data quality
- Popular data validation frameworks
- Implementing data validation with <span class="text-primary font-bold italic">Dagster</span>
- Supporting data quality standards in your organization

---
layout: statement
---

# What is data quality?

## Data quality is an indicator used to determine if data is suitable to be used in decision-making &mdash; whether that be for making business decisions, academic research, or generally finding meaningful insights.

---
layout: quote
---

## "Data is used to both run and improve the ways that the organization achieves their business objectives … [data quality ensures that] data is of sufficient quality to meet the business needs."

_The Practitioner's Guide to Data Quality Improvement, David Loshin_

---
layout: center
---

# Business decisions and operations

- Product development feature prioritization
- Risk assessment and fraud detection
- Efficient marketing campaigns
- Operational efficiency
    * Minimize rework / relieve engineering churn

---
layout: quote
---

# Financial impact

## It is estimated that poor data quality costs organizations $12.9 million / year

[[Gartner]](https://www.gartner.com/smarterwithgartner/how-to-improve-your-data-quality)

---
layout: center
---

# Trust

## By establishing a company culture around data-quality best practices, trust is formed amongst stakeholders, employees, customers, and partners.

---
layout: section
---

# The dimensions of data quality

<div class="grid grid-cols-3 gap-8">
  <div class="border-2 border-purple-300 rounded-lg font-bold text-xl p-4 text-center">Timeliness</div>
  <div class="border-2 border-purple-300 rounded-lg font-bold text-xl p-4 text-center">Completeness</div>
  <div class="border-2 border-purple-300 rounded-lg font-bold text-xl p-4 text-center">Accuracy</div>
  <div class="border-2 border-purple-300 rounded-lg font-bold text-xl p-4 text-center">Validity</div>
  <div class="border-2 border-purple-300 rounded-lg font-bold text-xl p-4 text-center">Consistency</div>
  <div class="border-2 border-purple-300 rounded-lg font-bold text-xl p-4 text-center">Uniqueness</div>
</div>

---
layout: center
---

<!--<ri-timer-flash-line class="text-primary h-24 w-24 abs-tr mt-36 mr-16" />-->

<h1 class="bg-purple-200 w-min px-2 italic">Timeliness</h1>

## Data is ready within a certain time frame

<hr class="my-4" />

- **Example**: an organization expects a financial report to land in an S3 bucket on a weekly basis,
    howeover, this report hasn't been received for the past month.
- **Impact**: data not received within the expected latency window  can result in operational impacts, and inaccuracies in analysis and reporting

<img src="/images/table_timeliness.png" class="h-42 abs-br mr-16" />

---
layout: default
---

<!--<ri-pie-chart-line class="text-primary h-24 w-24 abs-tr mt-10 mr-24" />-->

<h1 class="bg-purple-200 w-min px-2 italic">Completeness</h1>

## Data is fully populated in attributes and records

<hr class="my-4" />

- <span class="bg-yellow-100">No fields or attributes are missing from a database record</span>
    - **Example**: customer record — first name, last name, e-mail address
    - **Impact**: missing customer information can result in skewed analysis

- <span class="bg-yellow-100">No records are missing (e.g. transactional data)</span>
    - **Example**: a financial institution monitors a sequence of transactions between two parties, and an individual transaction is missing.
    - **Impact**: improper data reconciliation can result in failure to pass audits and compliance


<div class="absolute left-1/2 transform -translate-x-1/2 translate-y-4">
  <img src="/images/table_completeness.png" class="h-24"/>
</div>

---
layout: center
---

<!--<ri-focus-2-line class="text-primary h-24 w-24 abs-tr mt-14 mr-24" />-->

<h1 class="bg-purple-200 w-min px-2 italic">Accuracy</h1>

## Data values are aligned with a source of truth

<hr class="my-4" />

- <span class="bg-yellow-100">Categorical accuracy</span>
    - **Example**: Patient information in a medical database has incorrect values (blood type, allergies, etc)
    - **Impact**: incorrect data can result in dangerous decision making

- <span class="bg-yellow-100">Numerical accuracy</span>
    - **Example**: GPS location of a driver is off by 100 meters from their actual location
    - **Impact**: can result in incorrect representation for customers

<img src="/images/table_accuracy.png" class="h-28 abs-br mb-4" />

---
layout: center
---

<!--<ri-verified-badge-line class="text-primary h-24 w-24 abs-tr mt-12 mr-24" />-->

<h1 class="bg-purple-200 w-min px-2 italic">Validity</h1>

## Data values conform to an accepted format
 
<hr class="my-4" />

- <span class="bg-yellow-100">Categorical data entries adhere to expected list of values</span>
    - **Example**: A bank maintains a table of customer accounts, with account `type` field that can either be `checking` or `savings`, but a value is incorrectly entered as `loan`
    - **Impact**: Failures and errors in transactional processing

- <span class="bg-yellow-100">Values do not fit into a given structure or format</span>
    - **Example**: An e-commerce platform does not validate e-mail addresses at the application
    - **Impact**: Failed communication, bounced marketing e-mails, issues with account recovery processes

<img src="/images/table_validity.png" class="h-20 abs-br mb-38 mr-12" />

---

<!--<ri-layout-vertical-line class="text-primary h-24 w-24 abs-tr mt-12 mr-24" />-->

<h1 class="bg-purple-200 w-min px-2 italic">Consistency</h1>

## Data is aligned across systems and sources

<hr class="my-4" />

- <span class="bg-yellow-100">Data matches across systems (Example 1)</span>
    - **Example**: a data team replicates a postgres table to delta live tables for analytics
    - **Impact**: incorrect analysis of data can occur resulting in invalid metrics

- <span class="bg-yellow-100">Data matches across systems (Example 2)</span>
    - **Example**: a cryptocurrency exchange has an internal ledger that must match the public blockchain
    - **Impact**: incorrect balances can result in transactional errors, along with failed auditing

<div class="flex items-center justify-center mt-4">
  <img src="/images/table_consistency.png" class="h-26"/>
</div>

---
layout: center
---

<!--<ri-snowflake-fill class="text-primary h-24 w-24 abs-tr mt-36 mr-24" />-->

<h1 class="bg-purple-200 w-min px-2 italic">Uniqueness</h1>

## Data is free of duplicate values

<hr class="my-4" />

- **Example**: An online retailer maintains a list of products with the expectation that Stock Keeping Unit (SKUs) are unique, but there is a duplicate entry
- **Impact**: duplicate SKUs could result in inventory tracking errors, incorrect product listings, or order fulfillment mistakes

<div class="flex items-center justify-center mt-4">
  <img src="/images/table_uniqueness.png" class="h-32"/>
</div>

---
layout: section
---

# Tools and technology

---
layout: center
---

# Popular tools for data validation

- [Soda](https://github.com/sodadata/soda-core) - data quality testing for SQL-, Spark-, and Pandas-accessible data.

- [Great Expectations](https://github.com/great-expectations/great_expectations) - a data quality platform designed by and for data engineers

- [Deequ](https://github.com/awslabs/deequ) - a library built on top of Apache Spark for defining "unit tests for data", which measure data quality in large datasets.

- [dbt tests](https://docs.getdbt.com/docs/build/data-tests) - assertions you make about your models and other resources in your dbt project

- <span class="text-primary italic">[Dagster asset checks ⭐](https://docs.dagster.io/concepts/assets/asset-checks#asset-checks)</span> - code defined alongside assets to verify asset quality and derive conditional execution logic

---
layout: center
---

# Validation should occur at all stages of the data lifecycle

- Validate responses from APIs
- Data transformations in tools like `dbt`
- Confirm that data has been loaded successful to its destination
- External assets to validate health of BI tools, and external systems

---
layout: center
---

# Orchestration is a natural home for enforcing quality

- Encompasses all stages
- Pipelines can be made more resilient with conditional execution
- Quality can be more easily observed (single pane of glass)
    - alerts
    - visual indicators
- <span class="bg-yellow-100">Can be guided by platform owner</span>

<img src="/images/dagster-yay.gif"  class="abs-br mr-24 mb-48" />

---

<div class="flex justify-center">
  <img  src='/images/dagster-lineage.png' class="h-112 border-2 border-primary rounded-lg shadow-lg" />
</div>

---
layout: section
background: '/images/background.png'
---

# Dagster asset checks

Code-defined expectations for the data produced by your pipeline (e.g. data contracts) that can be used to apply conditional materialization

---

# Asset check example

```python {all|5-9|11-17|all}
import pandas as pd

from dagster import AssetCheckResult, Definitions, asset, asset_check

@asset
def orders():
    orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})
    orders_df.to_csv("orders.csv")


@asset_check(asset=orders)
def orders_id_has_no_nulls():
    orders_df = pd.read_csv("orders.csv")
    num_null_order_ids = orders_df["order_id"].isna().sum()
    return AssetCheckResult(
        passed=bool(num_null_order_ids == 0),
    )


defs = Definitions(
    assets=[orders],
    asset_checks=[orders_id_has_no_nulls],
)
``` 

---

<div class="flex justify-center">
  <img  src='/images/dagster-asset-check-1.png' class="border-2 border-primary rounded-lg shadow-lg" />
</div>

---

<div class="flex justify-center">
  <img  src='/images/dagster-asset-check-2.png' class="border-2 border-primary rounded-lg shadow-lg" />
</div>

---
layout: center
---

# "But I don't want to write those validation rules..."

---
layout: statement
---

## Leverage the frameworks you love!

<a class="bg-purple-200 px-2" href="https://dagster.io/blog/ensuring-data-quality-with-dagster-and-great-expectations">Blog Post: Enabling Data Quality with Dagster and Great Expectations - Muhammad Jarir Kanji</a>

<!--[Blog Post: Enabling Data Quality with Dagster and Great Expectations - Muhammad Jarir Kanji](https://dagster.io/blog/ensuring-data-quality-with-dagster-and-great-expectations)-->

---

# Great expectations resource

<img src="/images/great-expectations-logo.png" class="text-purple-400 h-8 abs-tr mt-12 mr-16 z-99" />

```python {*|7|8|12-17|*}
import pandas as pd

from dagster import ConfigurableResource
from great_expectations.data_context import EphemeralDataContext
from great_expectations.data_context.types.base import DataContextConfig, InMemoryStoreBackendDefaults

class GreatExpectationsResource(ConfigurableResource):
    def get_validator(self, asset_df: pd.DataFrame):

        # /* ... */

        data_context = EphemeralDataContext(project_config=project_config)
        batch_request = data_asset.build_batch_request(dataframe=asset_df)

        return data_context.get_validator(
            batch_request=batch_request, expectation_suite_name=suite_name
        )
```

---

```python {*|1|2-5|8,10-11|13-19|21-27|*}
@multi_asset_check(
    specs=[
        AssetCheckSpec(name="multicheck_target_has_no_nulls", asset=titanic),
        AssetCheckSpec(name="multicheck_target_has_valid_values", asset=titanic),
    ]
)
def ge_multiple_checks(
    context: AssetCheckExecutionContext, ge_resource: GreatExpectationsResource
):
    titanic_df = pd.read_csv("titanic.csv")
    validator = ge_resource.get_validator(titanic_df)

    validation_result = validator.expect_column_values_to_not_be_null(column="Survived")
    yield AssetCheckResult(
        passed=validation_result.success,
        severity=AssetCheckSeverity.ERROR,
        metadata=validation_result.result,
        check_name="multicheck_target_has_no_nulls",
    )

    validation_result = validator.expect_column_values_to_be_in_set(column="Survived", value_set={0, 1})
    yield AssetCheckResult(
        passed=validation_result.success,
        severity=AssetCheckSeverity.ERROR,
        metadata=validation_result.result,
        check_name="multicheck_target_has_valid_values",
    )
```

---

<div class="flex justify-center">
  <img  src='/images/dagster-asset-check-ge.png' class="border-2 border-primary rounded-lg shadow-lg" />
</div>

---
layout: section
---

# <span class="text-orange-500 italic">dbt tests</span> in Dagster

---

<img src="/images/dbt-logo.png" class="text-purple-400 h-8 abs-tr mt-12 mr-16 z-99" />

# dbt tests

```yaml {*|3|6-9|11-15|17-21|*}
# models/course_reviews.yml

name: course_reviews

columns:
  - name: course_review_id
    tests:
      - unique
      - not_null

  - name: course_id
    tests:
      - relationships:
          to: ref('courses')
          field: course_id

  - name: user_id
    tests:
      - relationships:
          to: ref('users')
          field: user_id
```

---

<div class="flex justify-center">
  <img  src='/images/dagster-asset-check-dbt.png' class="border-2 border-primary rounded-lg shadow-lg" />
</div>

---
layout: section
---

# Dagster <span class="text-green-300 italic">freshness</span> checks

---
layout: center
---

# Freshness checks

- Defines contract around SLA for data freshness
- Helps identify problems such as
    - A pipeline hitting an error and failing
    - Materializations or job runs never occurring
    - A backed up run queue
- Downstream assets can reference freshness checks on parents

---

# Implementing freshness checks

Assets are expected to be updated within 15 minutes from the current time

```python {*|6-11|13-15|*}
from datetime import timedelta

from dagster import AssetKey, Definitions, build_last_update_freshness_checks, build_sensor_for_freshness_checks


freshness_checks = build_last_update_freshness_checks(
    assets=[
        AssetKey(["internal", "main", "logs"]),
    ],
    lower_bound_delta=timedelta(minutes=15),
)

freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=freshness_checks
)

defs = Definitions(
    asset_checks=[*freshness_checks],
    sensors=[freshness_checks_sensor],
)
```

---

<div class="flex justify-center">
  <img  src='/images/dagster-freshness-checks.png' class="h-124 border-2 border-primary rounded-lg shadow-lg" />
</div>

---

# Implementing freshness checks (#2)

Assets are expected to be updated within 3 hours of the deadline

```python {*|6-11|13-15|*}
from datetime import timedelta

from dagster import AssetKey, build_last_update_freshness_checks, build_sensor_for_freshness_checks


checks_def = build_last_update_freshness_checks(
    [AssetKey("my_asset_key")],
    lower_bound_delta=timedelta(hours=3),
    deadline_cron="0 9 * * *",
)

sensor_def = build_sensor_for_freshness_checks(
    freshness_checks=checks_def
)

defs = Definitions(
    asset_checks=[*checks_def],
    sensors=[sensor_def],
)
```

---
layout: center
---

# Anomaly detection

Dagster+ Pro users can take advantage of a time series anomaly detection model applied to a selection of assets that references data from past materializations to determine if data is arriving late


```python
from dagster_cloud.anomaly_detection import build_anomaly_detection_freshness_checks

freshness_checks = build_anomaly_detection_freshness_checks(
  assets=[source_tables], params=None
)
```

[[docs]](https://docs.dagster.io/concepts/assets/asset-checks/checking-for-data-freshness#option-2-use-anomaly-detection-dagster-pro)

---
layout: section
---

# Asset health ❤️ dashboard

## A quick glance at all of your organizations assets

---

<div class="flex justify-center">
  <img  src='/images/dagster-asset-health-dashboard.png' class="h-124 border-2 border-primary rounded-lg shadow-lg" />
</div>

---
layout: section
---

# Promoting data quality standards in an organization

---
layout: center
---

# Establishing standards

- <span class="text-primary font-bold italic">Platform owners</span> and <span class="text-primary font-bold italic">governance teams</span> should establish frameworks that promote data enforcement and validation

- Data quality metrics and KPIs should be established with the help of domain experts

- A data aware culture should be fostered across the organization
    - Adhere to change-requests and reviews
    - Be curious and question results

- Regular audits and automated testing

---
layout: center
---

# Common challenges and pitfalls

- Managing data quality across distributed teams

- Retroactively enforcing standards and dealing with legacy systems

- Upfront developer cost of following data quality best practices

- Establishing ownership of data


---
layout: center
---

# Conclusion

- Having high quality data promotes <span class="text-yellow-600 font-bold">trust</span> in an organization
- There exist several dimensions to consider when enforcing data quality
- the orchestrator is a natural home to perform data validation
- It's a <span class="text-primary font-bold">team effort</span>, with data platform owners and governance teams playing a crucial role in establishing guidelines

---
layout: center
---

<h1 class="font-bold bg-purple-200 px-2">Next steps</h1>

<div class="my-10 grid grid-cols-[40px_1fr] gap-y-4 items-center">
  <img src="/images/dagsir.png" class="w-8" /><div><a href="https://docs.dagster.io/getting-started/quickstart" target="_blank">Download Dagster!</a></div>
  <img src="/images/dagster-cowboy.png" class="w-8" /><div><a href="https://docs.dagster.io/getting-started" target="_blank">Explore our blog posts and learning material</a></div>
  <img src="/images/dagster-bot-resolve-to-issue.png" class="w-8" /><div><a href="https://github.com/dagster-io/dagster/discussions" target="_blank">Join us on GitHub discussions</a></div>
  <img src="/images/dagster-bot-resolve-to-discussion.png" class="w-8" /><div><a href="https://dagster.io/slack" target="_blank">Chat with us (and <span>#ask-ai 🤖</span>) on Slack</a></div>
</div>

---
layout: center
---

<div class="flex items-center">
  <div class="font-bold text-primary text-6xl">Q & A</div>
  <img src="/images/dagster-primary-mark.svg" class="h-24" />
</div>
