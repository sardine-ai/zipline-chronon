namespace py ai.chronon.api.common
namespace java ai.chronon.api

// integers map to milliseconds in the timeunit
enum TimeUnit {
    HOURS = 0
    DAYS = 1
    MINUTES = 2
}

struct Window {
    1: i32 length
    2: TimeUnit timeUnit
}

enum ConfigType {
    STAGING_QUERY = 1
    GROUP_BY = 2
    JOIN = 3
    MODEL = 4
}

struct DateRange {
    1: string startDate
    2: string endDate
}

struct EnvironmentVariables {
    1: optional map<string, string> common
    2: optional map<string, string> backfill
    3: optional map<string, string> upload
    4: optional map<string, string> streaming
}

struct ConfigProperties {
    1: optional map<string, string> common
    2: optional map<string, string> backfill
    3: optional map<string, string> upload
    4: optional map<string, string> streaming
}

struct TableDependency {
    // fully qualified table name
    1: optional string table

    // params to select the partitions of the table for any query range
    // logic is: [max(query.start - startOffset, startCutOff), min(query.end - endOffset, endCutOff)]
    2: optional Window startOffset
    3: optional Window endOffset
    4: optional string startCutOff
    5: optional string endCutOff

    # if not present we will pull from defaults
    100: optional string partitionColumn
    101: optional string partitionFormat
    102: optional Window partitionInterval

    /**
    * If isCumulative is true, then for a given output partition any single partition from input on or after the output
    * is sufficient. What this means is that latest available partition prior to end cut off will be used.
    **/
    200: optional bool isCumulative

    /**
    * JoinParts could use data from batch backfill-s or upload tables when available
    * When not available they shouldn't force computation of the backfills and upload tables.
    **/
    201: optional bool forceCompute
}

enum KvScanStrategy {
    ALL = 0
    LATEST = 1
}

struct KvDependency {
    1: optional string cluster
    2: optional string table
    3: optional string keyBase64

    10: optional i64 startMillis
    11: optional i64 endMillis

    20: optional KvScanStrategy scanStrategy
}

struct ExecutionInfo {
    # information that needs to be present on every physical node
    1: optional EnvironmentVariables env
    2: optional ConfigProperties conf
    3: optional i64 dependencyPollIntervalMillis
    4: optional i64 healthCheckIntervalMillis

    # relevant for batch jobs
    10: optional string scheduleCron
    11: optional i32 stepDays
    12: optional bool historicalBackfill
    13: optional list<TableDependency> tableDependencies

    # relevant for streaming jobs
    200: optional list<KvDependency> kvDependency
    201: optional i64 kvPollIntervalMillis
    # note that batch jobs could in theory also depend on model training runs
    # in which case we will be polling
    # in the future we will add other types of dependencies
}