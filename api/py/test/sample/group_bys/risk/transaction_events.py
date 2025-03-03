from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, selects
from ai.chronon.group_by import GroupBy, Aggregation, Operation

"""
This GroupBy aggregates metrics about a user's previous purchases in various windows.
"""


def create_transaction_source(key_field):
    return Source(
        events=EventSource(
            table="data.txn_events",  # Points to the historical purchase events table
            topic=None,
            query=Query(
                selects=selects(key_field, "transaction_amount", "transaction_type"),
                time_column="transaction_time",
            ),
        )
    )


window_sizes = ["1h", "1d", "30d", "365d"]


def create_txn_group_by(source, key):
    return GroupBy(
        sources=[source],
        keys=[key],
        online=True,
        aggregations=[
            Aggregation(
                input_column="transaction_amount",
                operation=Operation.COUNT,
                windows=window_sizes,
            ),
            Aggregation(
                input_column="transaction_amount",
                operation=Operation.SUM,
                windows=["1h"],
            ),
        ],
    )


source_user_transactions = create_transaction_source("user_id")
txn_group_by_user = create_txn_group_by(source_user_transactions, "user_id")

source_merchant_transactions = create_transaction_source("merchant_id")
txn_group_by_merchant = create_txn_group_by(source_merchant_transactions, "merchant_id")
