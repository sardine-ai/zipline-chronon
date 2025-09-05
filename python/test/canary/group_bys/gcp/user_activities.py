from gen_thrift.api.ttypes import EventSource, Source
from staging_queries.gcp import exports

from ai.chronon.group_by import Aggregation, GroupBy, Operation, TimeUnit, Window
from ai.chronon.query import Query, selects

"""
This GroupBy aggregates user activity metrics from the user-activities-v0 topic.
It tracks various user behaviors (views, clicks, purchases, favorites, add_to_cart)
with last_k, sum, and average aggregations over multiple time windows.
"""

source = Source(
    events=EventSource(
        # This will be the BigQuery table that receives the PubSub data
        table=exports.user_activities.table,
        topic="pubsub://user-activities-v2/project=canary-443022/subscription=user-activities-v2-sub/serde=pubsub_schema/schemaId=user-activities",
        query=Query(
            selects=selects(
                user_id="user_id",
                listing_id="listing_id",
                # Create binary flags for each event type
                view_event="IF(event_type = 'view', 1, 0)",
                click_event="IF(event_type = 'click', 1, 0)", 
                purchase_event="IF(event_type = 'purchase', 1, 0)",
                favorite_event="IF(event_type = 'favorite', 1, 0)",
                add_to_cart_event="IF(event_type = 'add_to_cart', 1, 0)",
                # Device type flags
                is_mobile="IF(device_type = 'mobile', 1, 0)",
                is_desktop="IF(device_type = 'desktop', 1, 0)",
                is_tablet="IF(device_type = 'tablet', 1, 0)",
                # Activity structs for last_k tracking
                user_event_struct="STRUCT(event_type, listing_id, event_time_ms as timestamp)",
            ),
            time_column="event_time_ms",
        ),
    )
)

# Define window sizes for aggregations (1d, 7d, 14d, 30d)
window_sizes = [Window(length=days, time_unit=TimeUnit.DAYS) for days in [1, 7, 14, 30]]

# Event type columns for aggregations
event_columns = ["view_event", "click_event", "purchase_event", "favorite_event", "add_to_cart_event"]
device_columns = ["is_mobile", "is_desktop", "is_tablet"]
last_k_columns = ["user_event_struct"]
    
aggregations = []

# Event type aggregations - Sum and Average over various windows
aggregations.extend([
    Aggregation(input_column=col, operation=Operation.SUM, windows=window_sizes)
    for col in event_columns
])

aggregations.extend([
    Aggregation(input_column=col, operation=Operation.AVERAGE, windows=window_sizes)
    for col in event_columns
])

# Device type aggregations - Sum over various windows
aggregations.extend([
    Aggregation(input_column=col, operation=Operation.SUM, windows=window_sizes)
    for col in device_columns
])

# Last K aggregations - Keep track of recent activity patterns
aggregations.extend([
    Aggregation(input_column=col, operation=Operation.LAST_K(128), windows=window_sizes)
    for col in last_k_columns
])
    
v1 = GroupBy(
    sources=[source],
    keys=["user_id"],  # Aggregate by user
    online=True,
    version=0,
    aggregations=aggregations,
    step_days=4,
)
