
from dagster import MaterializeResult, MetadataValue, asset, AutoMaterializePolicy, AssetDep, BackfillPolicy, WeeklyPartitionsDefinition, MonthlyPartitionsDefinition, StaticPartitionsDefinition, AssetExecutionContext, AllPartitionMapping, TimeWindowPartitionMapping, DailyPartitionsDefinition

import requests
import json
import os

from .orders_data import get_dim_customers, get_dim_items, get_dim_promotions, get_dim_destinations, get_fct_orders, load_data

category_partition = StaticPartitionsDefinition(
    ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
)

daily_partition = DailyPartitionsDefinition(start_date='2020-01-01')
weekly_partition = WeeklyPartitionsDefinition(start_date='2020-01-01')
monthly_partition = MonthlyPartitionsDefinition(start_date='2020-01-01')


@asset
def dim_customers():
    get_dim_customers()

@asset(
    partitions_def=category_partition,
)
def dim_items(context: AssetExecutionContext):
    metadata = {}

    category = context.partition_key
    if category:
        context.log.info(f'Getting items for category: {category}')

    items = get_dim_items(category=category)

    # group by item_category and count
    category_count = items.groupby('category').size().to_dict()

    metadata = {
        **metadata,
        **{f'{k}': MetadataValue.int(v) for k, v in category_count.items()}
    }

    return MaterializeResult(metadata=metadata)

@asset
def dim_promotions():
    get_dim_promotions()

@asset
def dim_destinations():
    get_dim_destinations()

@asset(
    deps=[
        dim_customers, dim_promotions, dim_destinations,
        AssetDep(
            'dim_items',
            partition_mapping=AllPartitionMapping()
        )
    ],
    partitions_def=monthly_partition,
    backfill_policy=BackfillPolicy.single_run()
)
def fct_orders(context: AssetExecutionContext) -> MaterializeResult:
    start_date, end_date = context.partition_time_window
    data = get_fct_orders(start_date=start_date, end_date=end_date)

    return MaterializeResult(
        metadata={
            "order_count": MetadataValue.int(len(data)),
        }
    )


@asset(
    deps=[
        AssetDep(
            fct_orders,
            partition_mapping=TimeWindowPartitionMapping()
        )
    ],
    partitions_def=daily_partition,
    auto_materialize_policy=AutoMaterializePolicy.eager(
        max_materializations_per_minute=900
    )
)
def daily_top_items(context: AssetExecutionContext) -> MaterializeResult:
    start_date, end_date = context.partition_time_window
    orders = load_data('fct_orders', start_date=start_date, end_date=end_date)
    items = load_data('dim_items')

    # aggregate by item_id, then join with dim_items to get item_name
    daily_order_aggregates = orders.groupby('item_id').agg(
        order_count=('order_id', 'count'),
        total_amount=('amount', 'sum')
    ).merge(items, on='item_id').sort_values('order_count', ascending=False)

    daily_order_aggregates = daily_order_aggregates[['name', 'order_count', 'total_amount']]

    os.makedirs('data/daily_order_aggregates', exist_ok=True)
    daily_order_aggregates.to_parquet(f'data/daily_order_aggregates/{start_date.date()}-daily_order_aggregates.parquet', index=False)

    top_5_items = daily_order_aggregates.head(5).to_dict(orient='records')

    return MaterializeResult(
        metadata={
            "top_5_items": MetadataValue.json(top_5_items)
        }
    )

@asset(
    deps=[
        AssetDep(
            daily_top_items,
            partition_mapping=TimeWindowPartitionMapping()
        )
    ],
    partitions_def=weekly_partition,
    auto_materialize_policy=AutoMaterializePolicy.eager(
        max_materializations_per_minute=90
    )
)
def weekly_top_items(context: AssetExecutionContext) -> MaterializeResult:
    start_date, end_date = context.partition_time_window
    orders = load_data('fct_orders', start_date=start_date, end_date=end_date)
    items = load_data('dim_items')

    # aggregate by item_id, then join with dim_items to get item_name
    weekly_order_aggregates = orders.groupby('item_id').agg(
        order_count=('order_id', 'count'),
        total_amount=('amount', 'sum')
    ).merge(items, on='item_id').sort_values('order_count', ascending=False)

    weekly_order_aggregates = weekly_order_aggregates[['name', 'order_count', 'total_amount']]

    # write to parquet
    os.makedirs('data/weekly_order_aggregates', exist_ok=True)
    weekly_order_aggregates.to_parquet(f'data/weekly_order_aggregates/{start_date.date()}-weekly_order_aggregates.parquet', index=False)

    top_5_items = weekly_order_aggregates.head(5).to_dict(orient='records')

    return MaterializeResult(
        metadata={
            "top_5_items": MetadataValue.json(top_5_items)
        }
    )

@asset(
    deps=[
        AssetDep(
            weekly_top_items,
            partition_mapping=TimeWindowPartitionMapping()
        )
    ],
    partitions_def=monthly_partition,
    auto_materialize_policy=AutoMaterializePolicy.eager(
        max_materializations_per_minute=90
    )
)
def monthly_top_items(context: AssetExecutionContext) -> MaterializeResult:
    start_date, end_date = context.partition_time_window
    orders = load_data('fct_orders', start_date=start_date, end_date=end_date)
    items = load_data('dim_items')

    # aggregate by item_id, then join with dim_items to get item_name
    monthly_order_aggregates = orders.groupby('item_id').agg(
        order_count=('order_id', 'count'),
        total_amount=('amount', 'sum')
    ).merge(items, on='item_id').sort_values('order_count', ascending=False)

    monthly_order_aggregates = monthly_order_aggregates[['name', 'order_count', 'total_amount']]

    # write to parquet
    os.makedirs('data/monthly_order_aggregates', exist_ok=True)
    monthly_order_aggregates.to_parquet(f'data/monthly_order_aggregates/{start_date.date()}-monthly_order_aggregates.parquet', index=False)

    top_5_items = monthly_order_aggregates.head(5).to_dict(orient='records')

    return MaterializeResult(
        metadata={
            "top_5_items": MetadataValue.json(top_5_items)
        }
    )

# @asset(
#     deps=[fct_orders, dim_items],
# )
# def category_revenue() -> MaterializeResult:
#     fct_orders = load_data('fct_orders')
#     dim_items = load_data('dim_items')
#     category_revenue = {}

#     for category, group in fct_orders.merge(dim_items, on='item_id').groupby('category'):
#         category_revenue[category] = group['amount'].sum()

#     labels = list(category_revenue.keys())
#     values = list(category_revenue.values())

#     data = {"type": "bar", "data": {"labels": labels, "datasets": [{"label": "Category Revenue", "data": values, "backgroundColor": "rgba(75, 192, 192, 0.2)", "borderColor": "rgba(75, 192, 192, 1)", "borderWidth": 1}]}, "options": {"scales": {"y": {"beginAtZero": True}}}}

#     response = requests.get("https://quickchart.io/chart?c=" + json.dumps(data))

#     with open("data/category_revenue.png", "wb") as f:
#         f.write(response.content)

#     return MaterializeResult(
#         metadata={
#             "entry_data": MetadataValue.url(response.url)
#         }
#     )