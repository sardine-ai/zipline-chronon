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

from staging_queries.sample_team import sample_staging_query

from ai.chronon.query import Query, selects
from ai.chronon.types import EntitySource, EventSource
from ai.chronon.utils import get_staging_query_output_table_name


def basic_event_source(table):
    return EventSource(
        table=table,
        query=Query(
            selects=selects(
                event="event_expr",
                group_by_subject="group_by_expr",
            ),
            start_partition="2021-04-09",
            time_column="ts",
        ),
    )


# Sample Event Source used in tests.
event_source = EventSource(
    table="sample_namespace.sample_table_group_by",
    query=Query(
        selects=selects(
            event="event_expr",
            group_by_subject="group_by_expr",
            subject="subject",
        ),
        start_partition="2021-04-09",
        time_column="ts",
    ),
)

# Sample Entity Source
entity_source = EntitySource(
    snapshot_table="sample_table.sample_entity_snapshot",
    # hr partition is not necessary - just to demo that we support various
    # partitioning schemes
    mutation_table="sample_table.sample_entity_mutations/hr=00:00",
    mutation_topic="sample_topic",
    query=Query(
        start_partition="2021-03-01",
        selects=selects(
            group_by_subject="group_by_subject_expr",
            entity="entity_expr",
        ),
        time_column="ts",
    ),
)


batch_entity_source = EntitySource(
    snapshot_table="sample_table.sample_entity_snapshot",
    query=Query(
        start_partition="2021-03-01",
        selects=selects(
            group_by_subject="group_by_subject_expr",
            entity="entity_expr",
        ),
        time_column="ts",
    ),
)

sq_v1_selects = selects(
    **{
        "impressed_unique_count_1d": "impressed_unique_count_1d",
        "viewed_unique_count_1d": "viewed_unique_count_1d",
        "s2CellId": "s2CellId",
        "place_id": "place_id",
    }
)

# Sample Entity Source derived from a staging query.
staging_entities = EntitySource(
    snapshot_table="sample_namespace.{}".format(
        get_staging_query_output_table_name(sample_staging_query.v1)
    ),
    query=Query(
        start_partition="2021-03-01",
        selects=sq_v1_selects,
    ),
)


# A Source that was deprecated but still relevant (requires stitching).
events_until_20210409 = EventSource(
    table="sample_namespace.sample_table_group_by",
    query=Query(
        start_partition="2021-03-01",
        end_partition="2021-04-09",
        selects=selects(
            **{
                "group_by_subject": "group_by_subject_expr_old_version",
                "event": "event_expr_old_version",
            }
        ),
        time_column="UNIX_TIMESTAMP(ts) * 1000",
    ),
)


# The new source
events_after_20210409 = EventSource(
    table="sample_namespace.another_sample_table_group_by",
    query=Query(
        start_partition="2021-03-01",
        selects=selects(
            **{
                "group_by_subject": "possibly_different_group_by_subject_expr",
                "event": "possibly_different_event_expr",
            }
        ),
        time_column="__timestamp",
    ),
)
