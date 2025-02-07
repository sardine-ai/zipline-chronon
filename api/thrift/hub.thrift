namespace py ai.chronon.hub
namespace java ai.chronon.hub

include "common.thrift"
include "api.thrift"
include "orchestration.thrift"


/*
GroupBy APIs
*/


// For an entity-job page, we first call Lineage to get the node graph
// Then we call JobTrackerRequest on the first level of nodes to render the tasks for the landing page quickly
// Then we traverse the graph and load the rest of the tasks and statuses
// If the page was accessed via a "submission" link, then we also render the submission range
struct LineageRequest {
    1: optional string name
    2: optional string type // physical type (limited to backfill or batch upload)
    3: optional string branch
    4: optional Direction direction

}

struct LineageResponse {
    1: optional orchestration.NodeGraph nodeGraph
    2: optional orchestration.NodeKey mainNode // Same as the node in the LineageRequest
}

struct JobTrackerRequest {
   1: optional string name
   2: optional string type
   3: optional string branch
   10: optional DateRange dateRange // We may not need to use this, but in case it helps with page load times
}

struct JobTrackerResponse {
   1: optional list<TaskInfo> tasks // Date ranges can overlap for tasks (reruns, retries etc). Need to render latest per day.
   2: optional orchestration.NodeKey mainNode // Same as the node in the JobTrackerRequest
}

// Submissions are used to render user's recent jobs on their homepage
struct SubmissionsRequest {
    1: optional string user
}

struct SubmissionsResponse {
    1: optional list<Submission> submissions
}

enum Direction {
   UPSTREAM = 0,
   DOWNSTREAM = 1,
   BOTH = 2
}

struct TaskInfo {
    1: optional Status status
    2: optional string logPath
    3: optional string trackerUrl
    4: optional TaskArgs taskArgs
    5: optional DateRange dateRange // specific to batch nodes

    // time information - useful for gantt / waterfall view
    10: optional i64 submittedTs
    11: optional i64 startedTs
    12: optional i64 finishedTs

    20: optional string user
    21: optional string team

    // utilization information
    30: optional TaskResources allocatedResources
    31: optional TaskResources utilizedResources
}

struct DateRange {
    1: string startDate
    2: string endDate
}

struct TaskArgs {
  1: optional list<string> argsList
  2: optional map<string, string> env
}

struct TaskResources {
  1: optional i64 vcoreSeconds
  2: optional i64 megaByteSeconds
  3: optional i64 cumulativeDiskWriteBytes
  4: optional i64 cumulativeDiskReadBytes
 }


enum Mode {
    ADHOC = 0,
    SCHEDULED = 1
}

enum Status {
   WAITING_FOR_UPSTREAM = 0,
   WAITING_FOR_RESOURCES = 1,
   QUEUED = 2,
   RUNNING = 3,
   SUCCESS = 4,
   FAILED = 5,
   UPSTREAM_FAILED = 6,
   UPSTREAM_MISSING = 7
}

struct Submission {
    1: optional orchestration.NodeKey node
    10: optional i64 submittedTs
    20: optional i64 finishedTs
    21: optional DateRange dateRange
}

enum ConfType{
   STAGING_QUERY = 1
   GROUP_BY = 2
   JOIN = 3
   MODEL = 4
}

struct ConfRequest {
   1: optional string confName
   2: optional ConfType confType
   
   // one of either branch or version are set - otherwise we will pull conf for main branch
   3: optional string branch 
   4: optional string version
}

/**
  * lists all confs of the specified type
  */
struct ConfListRequest {
  1: optional ConfType confType
  
  // if not specified we will pull conf list for main branch
  2: optional string branch
}

/**
  * Response for listing configurations of a specific type
  */
struct ConfListResponse {
  1: optional list<api.Join> joins
  2: optional list<api.GroupBy> groupBys
  3: optional list<api.Model> models
  4: optional list<api.StagingQuery> stagingQueries
}
