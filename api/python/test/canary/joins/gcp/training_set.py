from group_bys.gcp.purchases import v1_dev, v1_test

from ai.chronon.api.ttypes import EventSource, Source
from ai.chronon.join import Join, JoinPart
from ai.chronon.query import Query, selects

"""
This is the "left side" of the join that will comprise our training set. It is responsible for providing the primary keys
and timestamps for which features will be computed.
"""
source = Source(
    events=EventSource(
        table="data.checkouts",
        query=Query(
            selects=selects(
                "user_id"
            ),  # The primary key used to join various GroupBys together
            time_column="ts",
        ),  # The event time used to compute feature values as-of
    )
)

v1_test = Join(
    left=source,
    right_parts=[
        JoinPart(group_by=v1_test)
    ],
)

v1_dev = Join(
    left=source,
    right_parts=[
        JoinPart(group_by=v1_dev)
    ],
)
