from group_bys.sample_team import (
    entity_sample_group_by_from_module,
    event_sample_group_by,
)
from sources import test_sources

import ai.chronon.types as ch

parent_join = ch.Join(
    left=test_sources.event_source,
    right_parts=[
        ch.JoinPart(
            group_by=event_sample_group_by.v1,
            key_mapping={"subject": "group_by_subject"},
        ),
        ch.JoinPart(
            group_by=entity_sample_group_by_from_module.v1,
            key_mapping={"subject": "group_by_subject"},
        ),
    ],
    online=True,
    check_consistency=True,
    historical_backfill=False,
)
