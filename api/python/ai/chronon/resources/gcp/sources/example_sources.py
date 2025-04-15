from ai.chronon.api.ttypes import EventSource, Source
from ai.chronon.query import Query, selects

"""
This GroupBy aggregates metrics about a user's previous purchases in various windows.
"""

# This source is raw purchase events. Every time a user makes a purchase, it will be one entry in this source.
source_v1 = Source(
    events=EventSource(
        table="data.purchases", # This points to the log table in the warehouse with historical purchase events, updated in batch daily
        topic=None, # See the 'returns' GroupBy for an example that has a streaming source configured. In this case, this would be the streaming source topic that can be listened to for realtime events
        query=Query(
            selects=selects("user_id","purchase_price"), # Select the fields we care about
            time_column="ts") # The event time
    ))
