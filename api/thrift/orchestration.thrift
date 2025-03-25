namespace py ai.chronon.orchestration
namespace java ai.chronon.orchestration

include "common.thrift"
include "api.thrift"

enum TabularDataType {
    EVENT = 1,
    ENTITY = 2,
    CUMULATIVE_EVENTS = 3,
    // SCD2 = 4,
}

/**
* Represents a group of structured data assets that the same data flows through
* just a normalized version of Events + Entity sources.
**/
struct TabularData {
    1: optional string table
    2: optional string topic
    3: optional string mutationTable
    4: optional TabularDataType type
}

union LogicalNode {
    1: api.StagingQuery stagingQuery
    2: api.Join join
    3: api.GroupBy groupBy
    4: api.Model model
    5: TabularData tabularData
}


enum LogicalType {
    GROUP_BY = 1,
    JOIN = 2,
    STAGING_QUERY = 3,
    MODEL = 4,
    TABULAR_DATA = 5
}

struct NodeKey {
    1: optional string name

    2: optional LogicalType logicalType
    3: optional PhysicalNodeType physicalType
}

struct NodeInfo {
    /**
    * represents the computation that a node does
    * direct changes to conf that change output will affect semantic hash
    * changing spark params etc shouldn't affect this
    **/
    11: optional string semanticHash

    /**
    * simple hash of the entire conf (that is TSimpleJsonProtocol serialized),
    * computed by cli and used to check if new conf_json need to be pushed from user's machine
    **/
    12: optional string confHash

    /**
    * when new/updated conf's are pushed the branch is also set from the cli
    * upon merging the branch will be unset
    **/
    20: optional string branch

    /**
    * will be set to the author of the last semantic change to node
    * (non-semantic changes like code-mods or spark params don't affect this)
    **/
    21: optional string author

    /**
    * contents of the conf itself
    **/
    30: optional LogicalNode conf
}



struct NodeConnections {
    1: optional list<NodeKey> parents
    2: optional list<NodeKey> children
}

struct NodeGraph {
    1: optional map<NodeKey, NodeConnections> connections
    2: optional map<NodeKey, NodeInfo> infoMap
}


// TODO deprecate
// ====================== physical node types ======================
enum GroupByNodeType {
    PARTIAL_IR = 1,  // useful only for events - a day's worth of irs
    SAWTOOTH_IR = 2, // realtime features: useful for join backfills & uploads
    SNAPSHOT = 3,    // batch features: useful for join backfills and uploads

    // online nodes
    PREPARE_UPLOAD = 10,
    UPLOAD = 11,
    STREAMING = 12,
}

enum JoinNodeType{
    BOOTSTRAP = 1,
    RIGHT_PART = 2,
    MERGE = 3,
    BACKFILL = 4,
    LABEL_PART = 5,
    LABEL_JOIN = 6,

    // online nodes
    METADATA_UPLOAD = 20,
    PREPARE_LOGS = 21,

    // observability nodes
    SUMMARIZE = 40,
    DRIFT = 41,
    DRIFT_UPLOAD = 42,
}

enum StagingQueryNodeType {
    BACKFILL = 1
}

enum ModelNodeType {
    TRAINING = 300
    BULK_INFERENCE = 301
}

enum TableNodeType {
    MATERIALIZED = 1,
    VIEW = 2
}

union PhysicalNodeType {
    1: GroupByNodeType groupByNodeType
    2: JoinNodeType joinNodeType
    3: StagingQueryNodeType stagingNodeType
    4: ModelNodeType modelNodeType
    5: TableNodeType tableNodeType
}

struct PhysicalNode {
    1: optional string name
    2: optional PhysicalNodeType nodeType
    3: optional LogicalNode logicalNode
    4: optional string confHash
    100: optional list<common.TableDependency> tableDependencies
    101: optional list<string> outputColumns
    102: optional string outputTable
}

struct PhysicalGraph {
    1: optional PhysicalNode node,
    2: optional list<PhysicalGraph> dependencies
    3: optional common.DateRange range
}

// ====================== End of physical node types ======================

/**
* Multiple logical nodes could share the same physical node
* For that reason we don't have a 1-1 mapping between logical and physical nodes
* TODO -- kill this (typescript dependency)
**/
struct PhysicalNodeKey {
    1: optional string name
    2: optional PhysicalNodeType nodeType
}

// ====================== End of physical node types ======================
// ====================== Modular Join Spark Job Args ======================

struct SourceWithFilterNode {
    1: optional api.Source source
    2: optional map<string,list<string>> excludeKeys
    10: optional api.MetaData metaData
}

struct JoinBootstrapNode {
    1: optional api.Join join
    10: optional api.MetaData metaData
}

struct JoinMergeNode {
    1: optional api.Join join
    10: optional api.MetaData metaData
}

struct JoinDerivationNode {
   1: optional api.Join join
   10: optional api.MetaData metaData
}

struct JoinPartNode {
    1: optional string leftSourceTable
    2: optional string leftDataModel
    3: optional api.JoinPart joinPart
    4: optional map<string, list<string>> skewKeys
    10: optional api.MetaData metaData
}

struct LabelPartNode {
    1: optional api.Join join
    10: optional api.MetaData metaData
}

union NodeUnion {
    1: SourceWithFilterNode sourceWithFilter
    2: JoinBootstrapNode joinBootstrap
    3: JoinPartNode joinPart
    4: JoinMergeNode joinMerge
    5: JoinDerivationNode joinDerivation
    // TODO add label join
    // TODO: add other types of nodes
}

// ====================== End of Modular Join Spark Job Args ===================

// ====================== Orchestration Service API Types ======================

struct Conf {
    1: optional string name
    2: optional string hash
    3: optional string contents
}

struct DiffRequest {
    1: optional map<string, string> namesToHashes
}

struct DiffResponse {
    1: optional list<string> diff
}

struct UploadRequest {
    1: optional list<Conf> diffConfs
    2: optional string branch
}

struct UploadResponse {
    1: optional string message
}

// ====================== End of Orchestration Service API Types ======================

/**
* Below are dummy thrift objects for execution layer skeleton code using temporal
* TODO: Need to update these to fill in all the above relevant fields
**/
struct DummyNode {
    1: optional string name
    2: optional list<DummyNode> dependencies
}

/**
* -- Phase 0 plan -- (same as chronon oss)
* StagingQuery::query - [deps.table] >> query
*
* GroupBy::upload     - [source.table] >> upload
* GroupBy::backfill   - [source.table] >> snapshot
*
* Join::label_join    - left.table, [bootstrap_part.table]?, [right.table], [label_part.table] >> label_join
*
*
* -- Phase 1 plan -- (broken up join)
* StagingQuery::query - [deps.table] >> query
*
* GroupBy::upload     - [source.table] >> upload
* GroupBy::backfill   - [source.table] >> snapshot
*
* Join::bootstrap     - left.table, [bootstrap_part.table]? >> bootstrap
* Join::right_part    - bootstrap, [right.table] >> right_part
* Join::merge         - bootstrap, [right_part] >> merge
* Join::derived       - merge >> derived
*
* Join::label_part    - bootstrap, [label_part.table] >> label_parts
* Join::label_join    - merge, [label_parts] >> label_join
*
*
* -- Phase 2 Plan -- sharing online w/ backfills (changes only)
* GroupBy::upload     - [source.table] >> sawtooth_ir/snapshot >> upload
* Join::right_part    - bootstrap, sawtooth_ir/snapshot(soft) + [right.table](fallback) >> right_part
*
*
* -- Phase 3 Plan -- incremental compute (changes only)
* GroupBy over events - [source.table] >> partial_ir >> robel_ir? >> sawtooth_ir/snapshot
*
*
* -- Phase 4 Plan -- model training
* Model::training          - [source.table] >> training
* Model::bulk_inference    - [source.table] >> bulk_inference
**/


/**
* physical node -> workflow id
*
*
* ad-hoc -> graph
*    we will trigger the root node with the right start_date and end_date
*
*
* Global Scheduler Workflow:
*   1. wakeup more frequently 15 minutes
*   2. scan database for unscheduled workflows
*   3. trigger unscheduled but required statuses
*
*
* Workflow is always triggered externally:
*
* node = get_node(name, version)
*
* node.trigger(start_date?, end_date, branch, is_scheduled):
*
*    # activity - 1
*    (missing_start, missing_end) = partition_dao.find_missing(start_date?, end_date)
*    missing_steps = compute_steps(missing_start, missing_end, branch_dao.get_step_days(this))
*
*    foreach_par missing_step in missing_steps:
*       foreach_par dependency in dependencies:
*
*           if dependency.is_internal:
*
*              (dep_start, dep_end) = dependency.compute_range(missing_step.start, missing_step.end)
*              # activity - 2
*              dependency.trigger_and_wait(dep_start, dep_end, branch)
*
*           else:
*
*              # activity - 3
*              if is_scheduled:
*                 dependency.wait(dep_start, dep_end)
*              else:
*                 dependency.fail_if_absent(dep_start, dep_end)
*
*       # activity - 4
*       node.submit_work_and_wait(missing_start, missing_end, branch_dao.get_conf(this))
*
*    return
*
*
*
*
* sync(physical_graph):
*
**/
