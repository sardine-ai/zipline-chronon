#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from staging_queries.kaggle.outbrain import base_table

from ai.chronon.api.ttypes import EventSource, Source
from ai.chronon.group_by import (
    Accuracy,
    Aggregation,
    GroupBy,
    Operation,
)
from ai.chronon.query import Query, selects
from ai.chronon.utils import get_staging_query_output_table_name

"""
This GroupBy aggregates clicks by the ad_id primary key, and it is setup to resemble a streaming GroupBy.

Normally, the source for a Streaming GroupBY would look something like:

EventSource(
    table="logging.clicks_event"
    topic="kafka_topic_for_click",
    ...)

However, in this case we're working with Static CSV data, so we don't have the right table/topics to set this up.

Instead, we're going to use the same output table from the staging query to perform the feature computation. Because it has all of
the fields that we care about (`ad_id`, `clicked`, and `ts` columns), it will mimic the offline data source correctly.
"""


source = Source(
    events=EventSource(
        table=get_staging_query_output_table_name(
            base_table
        ),  # Here we use the staging query output table because it has the necessary fields, but for a true streaming source we would likely use a log table
        topic="some_topic",  # You would set your streaming source topic here
        query=Query(selects=selects("ad_id", "clicked"), time_column="ts"),
    )
)

ad_streaming = GroupBy(
    sources=[source],
    keys=["ad_id"],  # We use the ad_id column as our primary key
    aggregations=[
        Aggregation(input_column="clicked", operation=Operation.SUM, windows=["3d"]),
        Aggregation(input_column="clicked", operation=Operation.COUNT, windows=["3d"]),
        Aggregation(
            input_column="clicked", operation=Operation.AVERAGE, windows=["3d"]
        ),
    ],
    accuracy=Accuracy.TEMPORAL,  # Here we use temporal accuracy so that training data backfills mimic streaming updates
)
