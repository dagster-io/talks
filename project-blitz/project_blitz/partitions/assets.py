from dagster import MaterializeResult, MetadataValue, asset, AutoMaterializePolicy, AssetDep, BackfillPolicy, WeeklyPartitionsDefinition, MonthlyPartitionsDefinition, StaticPartitionsDefinition, AssetExecutionContext, AllPartitionMapping, TimeWindowPartitionMapping, DailyPartitionsDefinition

import os

from .orders_data import get_dim_customers, get_dim_items, get_dim_promotions, get_dim_destinations, get_fct_orders, load_data

category_partition = StaticPartitionsDefinition(
    ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
)

START_DATE = '2023-11-01'

daily_partition = DailyPartitionsDefinition(start_date=START_DATE)
weekly_partition = WeeklyPartitionsDefinition(start_date=START_DATE)
monthly_partition = MonthlyPartitionsDefinition(start_date=START_DATE)


@asset(compute_kind="Snowflake", group_name="marts")
def dim_customers():
    """
    Table of customers and their information
    """
    get_dim_customers()

@asset(
    compute_kind="Snowflake",
    partitions_def=category_partition,
    group_name="marts"
)
def dim_items(context: AssetExecutionContext):
    """
    Items for sale, by category (ex. Electronics, Clothing, Books, Home, Sports)
    """
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

@asset(compute_kind="Snowflake", group_name="marts")
def dim_promotions():
    """
    Historical record of sales, discount codes, and promotions
    """
    get_dim_promotions()

@asset(compute_kind="Snowflake", group_name="marts")
def dim_destinations():
    """
    Where orders are being shipped to
    """
    get_dim_destinations()

@asset(
    compute_kind="Snowflake",
    deps=[
        dim_customers, dim_promotions, dim_destinations,
        AssetDep(
            dim_items,
            partition_mapping=AllPartitionMapping()
        )
    ],
    partitions_def=monthly_partition,
    backfill_policy=BackfillPolicy.single_run(),
    group_name="marts"
)
def fct_orders(context: AssetExecutionContext) -> MaterializeResult:
    """
    Monthly partitioned fact table for orders.
    """
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
            partition_mapping=TimeWindowPartitionMapping() # Note: Talk about what partition mappings is
        )
    ],
    partitions_def=daily_partition,
    compute_kind="Python",
    group_name="metrics",
    auto_materialize_policy=AutoMaterializePolicy.eager(
        max_materializations_per_minute=900
    )
)
def daily_top_items(context: AssetExecutionContext) -> MaterializeResult:
    """
    The best performing items for the day
    """
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
    compute_kind="Python",
    group_name="metrics",
    auto_materialize_policy=AutoMaterializePolicy.eager(
        max_materializations_per_minute=90
    )
)
def weekly_top_items(context: AssetExecutionContext) -> MaterializeResult:
    """
    The best performing items for the week, aggregated from the daily metrics
    """
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
    compute_kind="Python",
    group_name="metrics",
    auto_materialize_policy=AutoMaterializePolicy.eager(
        max_materializations_per_minute=90
    )
)
def monthly_top_items(context: AssetExecutionContext) -> MaterializeResult:
    """
    The best performing items for the month, aggregated from the weekly metrics
    """
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