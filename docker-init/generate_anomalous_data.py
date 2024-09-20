from decimal import Decimal
import random
from datetime import datetime, timedelta
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, FloatType, IntegerType, StringType, TimestampType, BooleanType, DecimalType
import pandas as pd
import boto3
import awswrangler as wr

# Initialize Spark session
spark = SparkSession.builder.appName("FraudClassificationSchema").getOrCreate()

ENDPOINT_URL = 'http://localhost:8000'

wr.config.dynamodb_endpoint_url = ENDPOINT_URL



def time_to_value(t, base_value, amplitude, noise_level, scale=1):
    if scale is None:
        return None
    hours = t.hour + t.minute / 60 + t.second / 3600
    x = hours / 24 * 2 * np.pi
    y = (np.sin(x) + np.sin(2*x)) / 2
    value = base_value + amplitude * y + np.random.normal(0, noise_level)
    return float(max(0, value * scale))

def generate_non_overlapping_windows(start_date, end_date, num_windows):
    total_days = (end_date - start_date).days
    window_lengths = [random.randint(3, 7) for _ in range(num_windows)]
    gap_days = random.randint(7, 30)
    gap = timedelta(days=gap_days)
    windows = []
    current_start = start_date + timedelta(days=random.randint(0, total_days - sum(window_lengths) - gap_days))
    for length in window_lengths:
        window_end = current_start + timedelta(days=length)
        if window_end > end_date:
            break
        windows.append((current_start, window_end))
        current_start = window_end + gap
        if current_start >= end_date:
            break
    
    return windows

def generate_timeseries_with_anomalies(num_samples=1000, base_value=100, amplitude=50, noise_level=10):
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    
    anomaly_windows = generate_non_overlapping_windows(start_date, end_date, 2)
    null_window, spike_window = anomaly_windows
    
    data = []
    time_delta = (end_date - start_date) / num_samples

    for i in range(num_samples):
        transaction_time = start_date + i * time_delta

        
        # Determine if we're in an anomaly window
        if null_window[0] <= transaction_time <= null_window[1]:
            scale = None
        elif spike_window[0] <= transaction_time <= spike_window[1]:
            scale = 5  # Spike multiplier
        else:
            scale = 1
        

        value = time_to_value(transaction_time, base_value=base_value, amplitude=amplitude, noise_level=noise_level, scale=scale)
        
        data.append((transaction_time, value))

    return data, {'null': null_window, 'spike': spike_window}


fraud_fields = [
    StructField("transaction_amount", DecimalType(), True),
    StructField("transaction_time", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("merchant_category_code", IntegerType(), True),
    StructField("account_age", IntegerType(), True),
    StructField("account_balance", DecimalType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("number_of_cards", IntegerType(), True),
    StructField("average_transaction_amount", DecimalType(), True),
    StructField("transaction_frequency", DecimalType(), True),
    StructField("number_of_devices", IntegerType(), True),
    StructField("unusual_hour_flag", BooleanType(), True),
    StructField("transaction_country", StringType(), True),
    StructField("distance_from_last_transaction", DecimalType(), True),
    StructField("ip_address_risk_score", DecimalType(), True),
    StructField("transactions_last_hour", IntegerType(), True),
    StructField("transactions_last_day", IntegerType(), True),
    StructField("amount_last_hour", DecimalType(), True),
    StructField("new_beneficiary_flag", BooleanType(), True),
    StructField("common_beneficiary_count", IntegerType(), True),
    StructField("device_type", StringType(), True),
    StructField("browser_language", StringType(), True),
    StructField("proxy_flag", BooleanType(), True),
    StructField("previous_fraud_flag", BooleanType(), True),
    StructField("days_since_last_transaction", IntegerType(), True)
]

fraud_schema = StructType(fraud_fields)
def generate_sample_data(num_samples=10000):
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
      
    data = []
    time_delta = (end_date - start_date) / num_samples
    # Generate base values
    transaction_amount, _ = generate_timeseries_with_anomalies(num_samples=num_samples, base_value=100, amplitude=50, noise_level=10)
    account_balance, _ = generate_timeseries_with_anomalies(num_samples=num_samples, base_value=5000, amplitude=2000, noise_level=500)
    average_transaction_amount, _ = generate_timeseries_with_anomalies(num_samples=num_samples, base_value=80, amplitude=30, noise_level=5)
    transaction_frequency, _ = generate_timeseries_with_anomalies(num_samples=num_samples, base_value=5, amplitude=3, noise_level=1)

    # print(len(transaction_amount), len(transaction_frequency), len(average_transaction_amount), len(account_balance))
    for i in range(num_samples):
        transaction_time = start_date + i * time_delta
        # Generate other features
        row = [
            Decimal(str(transaction_amount[i][1]) if transaction_amount[i][1] is not None else "0"),
            str(transaction_time),
            random.choice(['purchase', 'withdrawal', 'transfer']),
            random.randint(1000, 9999),
            random.randint(1, 3650),
            Decimal(str(account_balance[i][1]) if account_balance[i][1] is not None else "0"),
            random.randint(300, 850),
            random.randint(1, 5),
            Decimal(str(average_transaction_amount[i][1]) if average_transaction_amount[i][1] is not None else "0"),
            Decimal(str(transaction_frequency[i][1]) if transaction_frequency[i][1] is not None else "0"),
            random.randint(1, 5),
            random.choice([True, False]),
            random.choice(['US', 'UK', 'CA', 'AU', 'DE', 'FR']),
            Decimal(str(float(max(0, random.gauss(50, 30))))),
            Decimal(str(float(random.uniform(0, 1)))),
            random.randint(0, 10),
            random.randint(0, 50),
            Decimal(str(float(max(0, random.gauss(1000, 500))))),
            random.choice([True, False]),
            random.randint(0, 100),
            random.choice(['mobile', 'desktop', 'tablet']),
            random.choice(['en-US', 'es-ES', 'fr-FR', 'de-DE', 'zh-CN']),
            random.choice([True, False]),
            random.choice([True, False]),
            random.randint(0, 365)
        ]
        
        data.append(tuple(row))
    return data

data = generate_sample_data(20000)
df = spark.createDataFrame(data, schema=fraud_schema)
# df.show()

pandas_df = df.select("*").toPandas()

print(pandas_df.to_string())

dynamodb = boto3.client('dynamodb', endpoint_url=ENDPOINT_URL)
# boto_session = boto3.Session()
# dynamodb = boto_session.client('dynamodb', endpoint_url='http://localhost')
try:
    dynamodb.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'transaction_time',
                'AttributeType': 'S',
            },
        ],
        TableName='anomalous_data_table',
        KeySchema=[
            {
                'AttributeName': 'transaction_time',
                'KeyType': 'HASH'
            },
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 10,
            "WriteCapacityUnits": 10,
        },
    )
    print("Created anomalous data table")
except dynamodb.exceptions.ResourceInUseException:
    # This means it already exists.
    pass

wr.dynamodb.put_df(df=pandas_df, table_name="anomalous_data_table")
