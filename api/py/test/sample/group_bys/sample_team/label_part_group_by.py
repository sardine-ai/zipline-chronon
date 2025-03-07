from sources import test_sources
from ai.chronon.types import *

label_part_group_by = GroupBy(
    sources=test_sources.entity_source,
    keys=["group_by_subject"],
    aggregations=[
        Aggregation(
            input_column="group_by_subject",
            operation=Operation.SUM,
            windows=["7d"],
        ),
    ],
    online=False,
)

label_part_group_by_2 = GroupBy(
    sources=test_sources.batch_entity_source,
    keys=["group_by_subject"],
    aggregations=None,
    online=False,
)
