import random
from datetime import datetime, timedelta
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, TimestampType, BooleanType
import boto3
import awswrangler as wr
import os

# Initialize Spark session
spark = SparkSession.builder.appName("FraudClassificationSchema").getOrCreate()

ENDPOINT_URL = os.environ.get("DYNAMO_ENDPOINT") if os.environ.get("DYNAMO_ENDPOINT") is not None else 'http://localhost:8000'

wr.config.dynamodb_endpoint_url = ENDPOINT_URL
dynamodb = boto3.client('dynamodb', endpoint_url=ENDPOINT_URL)


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
    # join.source - txn_events
      StructField("user_id", IntegerType(), True),
      StructField("merchant_id", IntegerType(), True),

    # Contextual - 3
	StructField("transaction_amount", DoubleType(), True),
	StructField("transaction_time", TimestampType(), True),
	StructField("transaction_type", StringType(), True),

    # Transactions agg’d by user - 7 (txn_events)
	StructField("user_average_transaction_amount", DoubleType(), True),    
    StructField("user_transactions_last_hour", IntegerType(), True),
	StructField("user_transactions_last_day", IntegerType(), True),
    StructField("user_transactions_last_week", IntegerType(), True),
    StructField("user_transactions_last_month", IntegerType(), True),
    StructField("user_transactions_last_year", IntegerType(), True),
	StructField("user_amount_last_hour", DoubleType(), True),

    # Transactions agg’d by merchant - 7 (txn_events)
	StructField("merchant_average_transaction_amount", DoubleType(), True),    
    StructField("merchant_transactions_last_hour", IntegerType(), True),
	StructField("merchant_transactions_last_day", IntegerType(), True),
    StructField("merchant_transactions_last_week", IntegerType(), True),
    StructField("merchant_transactions_last_month", IntegerType(), True),
    StructField("merchant_transactions_last_year", IntegerType(), True),
	StructField("merchant_amount_last_hour", DoubleType(), True),

    # User features (dim_user) – 7
	StructField("user_account_age", IntegerType(), True),
	StructField("account_balance", DoubleType(), True),
	StructField("credit_score", IntegerType(), True),
    StructField("number_of_devices", IntegerType(), True),
    StructField("user_country", StringType(), True),
    StructField("user_account_type", IntegerType(), True),
    StructField("user_preferred_language", StringType(), True),

    # merchant features (dim_merchant) – 4
	StructField("merchant_account_age", IntegerType(), True),
	StructField("zipcode", IntegerType(), True),
    # set to true for 100 merchant_ids
    StructField("is_big_merchant", BooleanType(), True),
    StructField("merchant_country", StringType(), True),
    StructField("merchant_account_type", IntegerType(), True),      
    StructField("merchant_preferred_language", StringType(), True),


    # derived features - transactions_last_year / account_age - 1
	StructField("transaction_frequency_last_year", DoubleType(), True),
]

fraud_schema = StructType(fraud_fields)
def generate_fraud_sample_data(num_samples=10000):
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
      
    data = []
    time_delta = (end_date - start_date) / num_samples

    anomaly_windows = generate_non_overlapping_windows(start_date, end_date, 2)

    # Generate base values
    transaction_amount, _ = generate_timeseries_with_anomalies(num_samples=num_samples, base_value=100, amplitude=50, noise_level=10)
    account_balance, _ = generate_timeseries_with_anomalies(num_samples=num_samples, base_value=5000, amplitude=2000, noise_level=500)
    user_average_transaction_amount, _ = generate_timeseries_with_anomalies(num_samples=num_samples, base_value=80, amplitude=30, noise_level=5)
    merchant_average_transaction_amount, _ = generate_timeseries_with_anomalies(num_samples=num_samples, base_value=80, amplitude=30, noise_level=5)
    user_last_hour_list, _ = generate_timeseries_with_anomalies(num_samples=num_samples, base_value=5, amplitude=3, noise_level=1)
    merchant_last_hour_list, _ = generate_timeseries_with_anomalies(num_samples=num_samples, base_value=5, amplitude=3, noise_level=1)

    # print(len(transaction_amount), len(transaction_frequency), len(average_transaction_amount), len(account_balance))
    for i in range(num_samples):
        transaction_time = start_date + i * time_delta
        merchant_id = random.randint(1,250)
        if user_last_hour_list[i][1] is None:
            user_last_hour = user_last_hour_list[i][1]
            user_last_day = None
            user_last_week = None
            user_last_month = None
            user_last_year = None
        else:
            user_last_hour = int(user_last_hour_list[i][1])
            user_last_day = random.randint(user_last_hour, 100)
            user_last_week = random.randint(user_last_day, 500)
            user_last_month = random.randint(user_last_week, 1000)
            user_last_year = random.randint(user_last_month, 10000)
        user_account_age = random.randint(1, 3650)

        if merchant_last_hour_list[i][1] is None:
            merchant_last_hour = merchant_last_hour_list[i][1]
            merchant_last_day = None
            merchant_last_week = None
            merchant_last_month = None
            merchant_last_year = None
        else:
            merchant_last_hour = int(merchant_last_hour_list[i][1])
            merchant_last_day = random.randint(merchant_last_hour, 100)
            merchant_last_week = random.randint(merchant_last_day, 500)
            merchant_last_month = random.randint(merchant_last_week, 1000)
            merchant_last_year = random.randint(merchant_last_month, 10000)
        # Generate other features

        is_fast_drift = transaction_time > anomaly_windows[0][0] and transaction_time < anomaly_windows[0][1]
        is_slow_drift = transaction_time > anomaly_windows[1][0] and transaction_time < anomaly_windows[1][1]

        if is_fast_drift and user_last_hour is not None:
            user_last_hour *= 10
            user_last_day *= 10
            user_last_week *= 10
            user_last_month *= 10
            user_last_year *= 10

        if is_fast_drift and merchant_last_hour is not None:
            merchant_last_hour *= 10
            merchant_last_day *= 10
            merchant_last_week *= 10
            merchant_last_month *= 10
            merchant_last_year *= 10

        if is_slow_drift and user_last_hour is not None:
            user_last_hour = int(user_last_hour * (1.05)**((transaction_time-anomaly_windows[1][0])).days)
            user_last_day = int(user_last_day * (1.05)**((transaction_time-anomaly_windows[1][0])).days)
            user_last_week = int(user_last_week * (1.05)**((transaction_time-anomaly_windows[1][0])).days)
            user_last_month = int(user_last_month * (1.05)**((transaction_time-anomaly_windows[1][0])).days)
            user_last_year = int(user_last_year * (1.05)**((transaction_time-anomaly_windows[1][0])).days)

        if is_slow_drift and merchant_last_hour is not None:
            merchant_last_hour = int(merchant_last_hour * (1.05)**((transaction_time-anomaly_windows[1][0])).days)
            merchant_last_day = int(merchant_last_day * (1.05)**((transaction_time-anomaly_windows[1][0])).days)
            merchant_last_week = int(merchant_last_week * (1.05)**((transaction_time-anomaly_windows[1][0])).days)
            merchant_last_month = int(merchant_last_month * (1.05)**((transaction_time-anomaly_windows[1][0])).days)
            merchant_last_year = int(merchant_last_year * (1.05)**((transaction_time-anomaly_windows[1][0])).days)

        row = [
            # join.source - txn_events
            random.randint(1,100),
            merchant_id,

            # Contextual - 3
            transaction_amount[i][1],
            transaction_time,
            random.choice(['purchase', 'withdrawal', 'transfer']),

            # Transactions agg’d by user - 7 (txn_events)
            user_average_transaction_amount[i][1],
            user_last_hour,
            user_last_day,
            user_last_week,
            user_last_month,
            user_last_year,
            random.uniform(0,100.0),

            # Transactions agg’d by merchant - 7 (txn_events)
            merchant_average_transaction_amount[i][1],
            merchant_last_hour,
            merchant_last_day,
            merchant_last_week,
            merchant_last_month,
            merchant_last_year,
            random.uniform(0,1000.0),

            # User features (dim_user) – 7    
            user_account_age,
            account_balance[i][1],
            random.randint(300, 850),
            random.randint(1, 5),
            random.choice(['US', 'UK', 'CA', 'AU', 'DE', 'FR']) if not is_fast_drift  else random.choice(['US', 'UK', 'CA', 'BR', 'ET', 'GE']),
            random.randint(0, 100),
            random.choice(['en-US', 'es-ES', 'fr-FR', 'de-DE', 'zh-CN']),

            # merchant features (dim_merchant) – 4
            random.randint(1, 3650),
            random.randint(10000, 99999),
            merchant_id < 100, 
            random.choice(['US', 'UK', 'CA', 'AU', 'DE', 'FR'])  if not is_fast_drift  else random.choice(['US', 'UK', 'CA', 'BR', 'ET', 'GE']),
            random.randint(0, 100),
            random.choice(['en-US', 'es-ES', 'fr-FR', 'de-DE', 'zh-CN']),
            
            # derived features - transactions_last_year / account_age - 1
            user_last_year/user_account_age if user_last_year is not None else None,
]
        
        data.append(tuple(row))
    return data

fraud_data = generate_fraud_sample_data(20000)
fraud_df = spark.createDataFrame(fraud_data, schema=fraud_schema)

fraud_df.write.parquet("data.parquet")
print("Successfully wrote user data to parquet")
