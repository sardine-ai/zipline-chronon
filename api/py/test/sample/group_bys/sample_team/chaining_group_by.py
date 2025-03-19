from joins.sample_team.sample_chaining_join_parent import parent_join

import ai.chronon.types as ch

chaining_group_by_v1 = ch.GroupBy(
    sources=ch.JoinSource(
        join=parent_join,
        query=ch.Query(
            selects=ch.selects(
                event="event_expr",
                group_by_subject="group_by_expr",
            ),
            start_partition="2023-04-15",
            time_column="ts",
        ),
    ),
    keys=["user_id"],
    aggregations=[
        ch.Aggregation(input_column="event", operation=ch.Operation.LAST),
    ],
    accuracy=ch.Accuracy.TEMPORAL,
    online=True,
    production=True,
    table_properties={
        "sample_config_json": """{"sample_key": "sample_value"}""",
        "description": "sample description",
    },
    output_namespace="sample_namespace",
)
