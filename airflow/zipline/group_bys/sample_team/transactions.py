from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select
from ai.chronon.group_by import (
    GroupBy,
    Aggregation,
    Operation,
    Window,
    TimeUnit,
)


source = Source(
    events=EventSource(
        table="pubsub_events.transactions",
        topic="transactions",
        query=Query(
            selects=select(**{
                "user": "user",
                "merchant": "merchant",
                "is_refund": "IF(is_refund, 0.0, 1.0)",
            }),
            time_column="ts")
    ))

v1 = GroupBy(
    sources=[source],
    keys=["user"],
    aggregations=[Aggregation(
            input_column="is_refund",
            operation=Operation.AVERAGE,
            windows=[Window(length=d, timeUnit=TimeUnit.DAYS) for d in [1, 7, 30]]
        )
    ],
)
