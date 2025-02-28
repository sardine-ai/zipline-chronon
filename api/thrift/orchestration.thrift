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
    PRE_DERIVATION = 3,
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
struct PhysicalNode {
    1: optional string name
    2: optional PhysicalNodeType nodeType

    /**
    * parentLineageHashes[] + semanticHash of the portion of compute this node does
    **/
    20: optional string lineageHash
    3: optional LogicalNode config



    21: optional string branch

    // null means ad-hoc, zero means continuously running
    40: optional common.Window scheduleInterval

    60: optional Artifact output
    61: optional list<Dependency> dependencies
}

struct PhysicalNodeInstance {
    1: optional PhysicalNode node
    2: optional map<Dependency, ArtifactRange> dependencyRanges
    3: optional ArtifactRange outputRange
}

union Artifact {
    1: optional Table table
    2: optional KvEntry kvEntry
    // could also be a topic, or a blog store location with model weights or image files etc
}

struct Table {
    1: optional string table
//    2: optional string database
//    3: optional string format
}

struct KvEntry {
    // cluster/table/key uniquely identifies the kv entry
    1: optional string cluster
    2: optional string table
    3: optional string keyAsBase64
}

enum ScanStrategy {
    ALL = 0
    LATEST = 1
    SKIP = 2
}

struct TableRange {
    1: optional Table table
    2: optional string startPartition
    3: optional string endPartition
    4: optional ScanStrategy scanStrategy
}

struct KvRange {
    1: optional KvEntry kvEntry
    2: optional i64 startMillis
    3: optional i64 endMillis
    4: optional ScanStrategy scanStrategy
}

union ArtifactRange {
    1: optional TableRange tableRange
    2: optional KvRange kvRange
}

struct KvDependency {
    1: optional KvEntry kvEntry
    2: optional common.Window startOffset
    3: optional common.Window endOffset
    4: optional ScanStrategy scanStrategy
}

struct TableDependency {
    1: optional Table table

    // params to select the partitions of the table for any query range
    // logic is: [max(query.start - startOffset, startCutOff), min(query.end - endOffset, endCutOff)]
    2: optional common.Window startOffset
    3: optional common.Window endOffset
    4: optional string startCutOff
    5: optional string endCutOff
    6: optional map<string, string> partitionFilters

    /**
    * If isCumulative is true, then for a given output partition any single partition from input on or after the output
    * is sufficient. What this means is that latest available partition prior to end cut off will be used.
    **/
    20: optional bool isCumulative

    /**
    * JoinParts could use data from batch backfills or upload tables when available
    * When not available they shouldn't force computation of the backfills and upload tables.
    **/
    21: optional bool forceCompute
}

union Dependency {
    1: optional KvDependency kvDependency
    2: optional TableDependency tableDependency
}

/**
* Below are dummy thrift objects for execution layer skeleton code using temporal
* TODO: Need to update these to fill in all the above relevant fields
**/

struct DummyNode {
    1: optional string name
    2: optional list<DummyNode> dependencies
}

struct DummyNodeGraph {
    1: optional list<DummyNode> leafNodes
    2: optional map<string, DummyNode> infoMap
}

struct DagExecutionWorkflowState {
    1: optional i64 processedDagCount
    2: optional i32 maxHistoryLength
    3: optional list<DummyNodeGraph> pendingDags
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