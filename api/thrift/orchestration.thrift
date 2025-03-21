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

    /**
    * represents the computation of the node including the computation of all its parents
    * direct and indirect changes that change output will affect lineage hash
    **/
    10: optional string lineageHash
}

struct NodeInfo {
   1: optional string name
   2: optional string contentHash
   3: optional string lineageHash
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
**/
struct PhysicalNodeKey {
    1: optional string name
    2: optional PhysicalNodeType nodeType

    /**
    * parentLineageHashes[] + semanticHash of the portion of compute this node does
    **/
    20: optional string lineageHash

}

// ====================== End of physical node types ======================
// ====================== Modular Join Spark Job Args ======================

struct SourceWithFilterNode {
    1: optional api.Source source
    2: optional map<string,list<string>> excludeKeys
}

struct JoinBootstrapNode {
    1: optional api.Join join
    2: optional string leftSourceTable
}

struct JoinMergeNode {
    1: optional api.Join join
    2: optional string leftInputTable
    3: optional map<api.JoinPart, string> joinPartsToTables
}

struct JoinDerivationNode {
   1: optional string trueLeftTable
   2: optional string baseTable
   3: optional list<api.Derivation> derivations
}

struct JoinPartNode {
    1: optional string leftSourceTable
    2: optional string leftDataModel
    3: optional api.JoinPart joinPart
    4: optional map<string, list<string>> skewKeys
}

struct LabelPartNode {
    1: optional api.Join join
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

struct NodeArgs {
   1: optional common.DateRange range
   2: optional string outputTable
   3: optional map<string, string> inputTableToVersionedTable
   4: optional common.ExecutionInfo executionInfo
}

struct NodeWithArgs {
    1: optional NodeUnion nodeUnion
    2: optional NodeArgs args
}

// ====================== End of Modular Join Spark Job Args ===================

// ====================== Orchestration Service API Types ======================

struct DiffRequest {
    1: optional map<string, string> namesToHashes
}

struct DiffResponse {
    1: optional list<string> diff
}

struct UploadRequest {
    1: optional list<PhysicalNode> diffNodes
    2: optional string branch
    3: optional list<NodeInfo> nodeInfos
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
