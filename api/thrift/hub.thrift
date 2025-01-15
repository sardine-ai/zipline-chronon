namespace py ai.chronon.hub
namespace java ai.chronon.hub

include "common.thrift"
include "api.thrift"
include "orchestration.thrift"


/*
GroupBy APIs
*/

/*
JobTrackerRequests can be for
- GroupBy, Backfill
- GroupBy, Upload
- Join, Backfill
- Join, Upload
- StagingQuery, Backfill

Streaming job metadata handled differently
 */
struct JobTrackerRequest {
    1: required orchestration.NodeKey node
    2: required Direction direction
    3: optional string startDate // Do we need a way to specify a range, in case of very large jobs?
    4: optional string endDate
}

struct JobTrackerResponse {
    1: map<orchestration.NodeKey, list<StepRange>> ranges
    2: required orchestration.NodeGraph nodeGraph
    3: required orchestration.NodeKey mainNode
}

enum Direction {
    UPSTREAM = 0,
    DOWNSTREAM = 1,
    BOTH = 2
}

struct StepRange {
    1: required string start
    2: required string end
    3: required jobStatus status
    4: required string jobId
}


enum jobStatus {
    WAITING_FOR_UPSTREAM = 0,
    WAITING_FOR_RESOURCES = 1,
    RUNNING = 2,
    SUCCESS = 3,
    FAILED = 4,
    UPSTREAM_FAILED = 5
    UPSTREAM_MISSING = 6
}
