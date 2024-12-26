from ai.chronon.api.ttypes import Source, EntitySource
from ai.chronon.query import Query, select
from ai.chronon.group_by import (
    GroupBy
)

"""
This GroupBy aggregates metrics about a user's previous purchases in various windows.
"""

# This source is raw purchase events. Every time a user makes a purchase, it will be one entry in this source.
source_users = Source(
    entities=EntitySource(
        snapshotTable="data.users", # This points to the log table in the warehouse with historical purchase events, updated in batch daily
        query=Query(
            selects=select("user_id","account_age", "account_balance", "credit_score", "number_of_devices", "country", "account_type", "preferred_language"), # Select the fields we care about
        ) # The event time
    )
)

user_group_by = GroupBy(
    sources=[source_users],
    keys=["user_id"],
    aggregations=None
)