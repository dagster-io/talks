import time
from faker import Faker
import random
import pandas as pd
import os
from datetime import datetime
from typing import Optional, List, Tuple
from pandas import DataFrame
from pytz import utc
fake = Faker()

MIN_DATE = datetime(2020, 1, 1, tzinfo=utc)
MAX_DATE = datetime.now(tz=utc)

CATEGORIES = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']

def get_date_bucket(date: datetime) -> Tuple[int, int]:
    month: int = date.month
    year: int = date.year
    return month, year

def increment_month(date: datetime) -> datetime:
    month = date.month
    year = date.year
    if month == 12:
        month = 1
        year += 1
    else:
        month += 1
    return datetime(year, month, 1, tzinfo=utc)

def load_data(table_name: str, start_date: datetime = MIN_DATE, end_date: datetime = MAX_DATE) -> DataFrame:
    dataframes = []

    current_date = start_date
    while current_date <= end_date:
        month, year = get_date_bucket(current_date)
        file_path = f'data/{table_name}/{year}-{month:02d}-{table_name}.parquet'
        if os.path.exists(file_path):
            df = pd.read_parquet(file_path)
            dataframes.append(df)
        current_date = increment_month(current_date)
    return pd.concat(dataframes, ignore_index=True)

def get_dim_customers(start_date: datetime = MIN_DATE, end_date: datetime = MAX_DATE):
    os.makedirs('data/dim_customers', exist_ok=True)

    current_date = start_date
    while current_date <= end_date:
        month, year = get_date_bucket(current_date)

        dim_customers: List[Tuple[str, str, str, str, str]] = []
        for _ in range(500):  # Generating 500 customers for example
            customer_id: str = fake.uuid4()
            name: str = fake.name()
            address: str = fake.address().replace('\n', ' ')  # Remove newline characters from address
            phone: str = fake.phone_number()
            email: str = fake.email()
            dim_customers.append((customer_id, name, address, phone, email))

        dim_customers_df: DataFrame = pd.DataFrame(dim_customers, columns=['customer_id', 'name', 'address', 'phone', 'email'])
        dim_customers_df.to_parquet(f'data/dim_customers/{year}-{month:02d}-dim_customers.parquet', index=False)
        print(f'data/dim_customers/{year}-{month:02d}-dim_customers.parquet')

        current_date = increment_month(current_date)

    return load_data('dim_customers', start_date, end_date)

def get_dim_items(start_date: datetime = MIN_DATE, end_date: datetime = MAX_DATE, category: Optional[str] = None):
    os.makedirs('data/dim_items', exist_ok=True)

    current_date = start_date
    while current_date <= end_date:
        month, year = get_date_bucket(current_date)

        dim_items: List[Tuple[str, str, str, str, float, str]] = []
        for _ in range(100):  # Generating 100 items for example
            item_id: str = fake.uuid4()
            name: str = fake.word().capitalize()
            description: str = fake.sentence()
            item_category: str = fake.random_element(elements=CATEGORIES)
            marketing_priority: str = 'low'
            if item_category == 'Electronics':
                marketing_priority = 'high'
            elif item_category == 'Clothing':
                marketing_priority = 'medium'
            elif item_category == 'Books':
                marketing_priority = 'low'
            elif item_category == 'Home':
                marketing_priority = 'medium'
            elif item_category == 'Sports':
                marketing_priority = 'high'
            price: float = random.uniform(10, 500)
            dim_items.append((item_id, name, description, item_category, price, marketing_priority))

        dim_items_df: DataFrame = pd.DataFrame(dim_items, columns=['item_id', 'name', 'description', 'category', 'price', 'marketing_priority'])
        dim_items_df.to_parquet(f'data/dim_items/{year}-{month:02d}-dim_items.parquet', index=False)
        print(f'data/dim_items/{year}-{month:02d}-dim_items.parquet')

        current_date = increment_month(current_date)

    return load_data('dim_items', start_date, end_date)

def get_dim_promotions(start_date: datetime = MIN_DATE, end_date: datetime = MAX_DATE):
    os.makedirs('data/dim_promotions', exist_ok=True)

    current_date = start_date
    while current_date <= end_date:
        month, year = get_date_bucket(current_date)

        dim_promotions: List[Tuple[str, str, str, datetime, datetime, str]] = []
        for _ in range(10):  # Generating 10 promotions for example
            promotion_id: str = fake.uuid4()
            name: str = fake.word().capitalize()
            promotion_type: str = fake.random_element(elements=('Discount', 'Coupon', 'Sale'))
            promotion_starts_at: datetime = fake.date_between_dates(date_start=datetime(year, month, 1), date_end=datetime(year, month, 28))
            promotion_ends_at: datetime = fake.date_between_dates(date_start=promotion_starts_at, date_end=datetime(year, month, 28))
            description: str = fake.sentence()
            dim_promotions.append((promotion_id, name, promotion_type, promotion_starts_at, promotion_ends_at, description))

        dim_promotions_df: DataFrame = pd.DataFrame(dim_promotions, columns=['promotion_id', 'name', 'promotion_type', 'promotion_starts_at', 'promotion_ends_at', 'description'])
        dim_promotions_df.to_parquet(f'data/dim_promotions/{year}-{month:02d}-dim_promotions.parquet', index=False)
        print(f'data/dim_promotions/{year}-{month:02d}-dim_promotions.parquet')

        current_date = increment_month(current_date)

    return load_data('dim_promotions', start_date, end_date)

def get_dim_destinations(start_date: datetime = MIN_DATE, end_date: datetime = MAX_DATE):
    os.makedirs('data/dim_destinations', exist_ok=True)

    current_date = start_date
    while current_date <= end_date:
        month, year = get_date_bucket(current_date)

        dim_destinations: List[Tuple[str, str, str, str, str]] = []
        for _ in range(100):  # Generating 5 destinations for example
            destination_id: str = fake.uuid4()
            name: str = fake.company()
            address: str = fake.address().replace('\n', ' ')  # Remove newline characters from address
            phone: str = fake.phone_number()
            email: str = fake.email()
            dim_destinations.append((destination_id, name, address, phone, email))

        dim_destinations_df: DataFrame = pd.DataFrame(dim_destinations, columns=['destination_id', 'name', 'address', 'phone', 'email'])
        dim_destinations_df.to_parquet(f'data/dim_destinations/{year}-{month:02d}-dim_destinations.parquet', index=False)
        print(f'data/dim_destinations/{year}-{month:02d}-dim_destinations.parquet')

        current_date = increment_month(current_date)

    return load_data('dim_destinations', start_date, end_date)

def get_fct_orders(start_date: datetime = MIN_DATE, end_date: datetime = MAX_DATE, order_count: int = 1000):
    os.makedirs('data/fct_orders', exist_ok=True)

    current_date = start_date
    while current_date <= end_date:
        month, year = get_date_bucket(current_date)

        dim_customers_df: DataFrame = pd.read_parquet(f'data/dim_customers/{year}-{month:02d}-dim_customers.parquet')
        dim_items_df: DataFrame = pd.read_parquet(f'data/dim_items/{year}-{month:02d}-dim_items.parquet')
        dim_promotions_df: DataFrame = pd.read_parquet(f'data/dim_promotions/{year}-{month:02d}-dim_promotions.parquet')
        dim_destinations_df: DataFrame = pd.read_parquet(f'data/dim_destinations/{year}-{month:02d}-dim_destinations.parquet')

        fct_orders: List[Tuple[str, str, str, datetime, int, float, str, str]] = []
        for _ in range(order_count):  # Generating 1000 orders for example
            order_id: str = fake.uuid4()
            customer_id: str = random.choice(dim_customers_df['customer_id'])
            order_date: datetime = fake.date_time_between_dates(datetime_start=start_date, datetime_end=end_date)
            promotion_id: str = random.choice(dim_promotions_df['promotion_id'])
            destination_id: str = random.choice(dim_destinations_df['destination_id'])
            num_items: int = random.randint(1, 5)  # Random number of items per order
            for _ in range(num_items):
                item_id: str = random.choice(dim_items_df['item_id'])
                quantity: int = random.randint(1, 10)
                amount: float = random.uniform(10, 1000)
                fct_orders.append((order_id, customer_id, item_id, order_date, quantity, amount, promotion_id, destination_id))

        fct_orders_df: DataFrame = pd.DataFrame(fct_orders, columns=['order_id', 'customer_id', 'item_id', 'order_date', 'quantity', 'amount', 'promotion_id', 'destination_id'])
        fct_orders_df.to_parquet(f'data/fct_orders/{year}-{month:02d}-fct_orders.parquet', index=False)
        print(f'data/fct_orders/{year}-{month:02d}-fct_orders.parquet')

        time.sleep(0.2)

        current_date = increment_month(current_date)

    return load_data('fct_orders', start_date, end_date)

def _generate_data(start_date: datetime = MIN_DATE, end_date: datetime = MAX_DATE):
    get_dim_customers(start_date, end_date)
    get_dim_items(start_date, end_date)
    get_dim_promotions(start_date, end_date)
    get_dim_destinations(start_date, end_date)
    get_fct_orders(start_date, end_date)

if __name__ == "__main__":
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    _generate_data(start_date, end_date)