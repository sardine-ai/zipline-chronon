from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, selects

source = EventSource(
    table="data.plaid_raw",
    topic=None,
    query=Query(
        selects=selects(
            "request_ip_v4_address",
            "fingerprint_pro_data_ip_v4_datacenter_ip",
            "user_agent_browser",
            "fingerprint_pro_data_ip_v4_latitude",
        ),
        time_column="UNIX_TIMESTAMP(ts) * 1000" # ts is in microseconds, convert to millis
    )
)

v1 = GroupBy(
    backfill_start_date="20250216",
    online=True,
    sources=[source],
    keys=["request_ip_v4_address"],
    aggregations=[
        Aggregation(
            input_column="fingerprint_pro_data_ip_v4_datacenter_ip",
            operation=Operation.LAST,
        ),
        Aggregation(
            input_column="user_agent_browser",
            operation=Operation.LAST_K(5),
        ),
        Aggregation(
            input_column="user_agent_browser",
            operation=Operation.APPROX_UNIQUE_COUNT,
        ),
        Aggregation(
            input_column="fingerprint_pro_data_ip_v4_latitude",
            operation=Operation.LAST,
        ),

    ]

)