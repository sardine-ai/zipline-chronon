from sources import test_sources

import ai.chronon.types as ch

label_part_group_by = ch.GroupBy(
    sources=test_sources.entity_source,
    keys=["group_by_subject"],
    aggregations=[
        ch.Aggregation(
            input_column="group_by_subject",
            operation=ch.Operation.SUM,
            windows=["7d"],
        ),
    ],
    online=False,
)

label_part_group_by_2 = ch.GroupBy(
    sources=test_sources.batch_entity_source,
    keys=["group_by_subject"],
    aggregations=None,
    online=False,
)
