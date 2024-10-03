from ai.chronon.api.ttypes import Source, EntitySource
from ai.chronon.query import Query, select
from ai.chronon.group_by import (
    GroupBy,
    Aggregation,
    Operation,
    Window,
    TimeUnit
)

"""
This GroupBy aggregates metrics about a user's previous purchases in various windows.
"""

# This source is raw purchase events. Every time a user makes a purchase, it will be one entry in this source.
source_merchants = Source(
    entities=EntitySource(
        snapshotTable="data.merchants", # This points to the log table in the warehouse with historical purchase events, updated in batch daily
        query=Query(
            selects=select("merchant_id","account_age", "zipcode", "is_big_merchant", "country", "account_type", "preferred_language"), # Select the fields we care about
        )
    )
)

merchant_group_by = GroupBy(
    sources=[source_merchants],
    keys=["merchant_id"],
    aggregations=None
)