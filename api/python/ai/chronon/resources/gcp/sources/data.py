from ai.chronon.api.ttypes import EventSource, Source
from ai.chronon.query import Query, selects

"""
Example: Defining a Chronon Source from a Batch Table

This example demonstrates how to configure a Chronon `Source` from a BigQuery or Hive table,
with a clear event time column and selected fields for downstream feature computation.
"""

# Define the EventSource using the batch table and query
# Wrap the EventSource in a Source object
source_v1 = Source(events=EventSource(
    table="my_dataset.user_events",
    topic=None,  # For real-time sources, provide a Kafka or Pub/Sub topic
    query=Query(
        selects=selects("user_id", "event_type", "event_timestamp"),
        time_column="event_timestamp",
        partition_column="ds",
    )
))

# The `source_v1` object can now be used in a Chronon join or pipeline definition
