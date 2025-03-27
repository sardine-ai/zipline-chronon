import ai.chronon.api.ttypes as thrift

source = thrift.Source(
    events=thrift.EventSource(
        table="etsy_search.visit_id_beacons",
        query=thrift.Query(
            selects={
                "event_name": "beacon.event_name",
                "listing_id": "beacon.properties['listing_id']",
            },
            timeColumn="beacon.timestamp",
        ),
    )
)
