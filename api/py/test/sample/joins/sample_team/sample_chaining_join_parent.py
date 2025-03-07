from sources import test_sources
from group_bys.sample_team import (
    event_sample_group_by,
    entity_sample_group_by_from_module,
)

from ai.chronon.types import *

parent_join = Join(
    left=test_sources.event_source,
    right_parts=[
        JoinPart(
            group_by=event_sample_group_by.v1,
            key_mapping={"subject": "group_by_subject"},
        ),
        JoinPart(
            group_by=entity_sample_group_by_from_module.v1,
            key_mapping={"subject": "group_by_subject"},
        ),
    ],
    online=True,
    check_consistency=True,
    historical_backfill=False,
)
