namespace py ai.chronon.lineage
namespace java ai.chronon.lineage

struct Column {
    1: optional string logicalNodeName
    2: optional ColumnLineageStage stageName
    3: optional string columnName
}

enum ColumnLineageStage {
    SELECT,
    AGG,
    DERIVE,
    JOIN
}


/**
    * NOTE: Staging Query will only be purely output columns.
    *       stage = SELECT, output = col_name, expression = null, input_cols = null, type = DIRECT
    *
    * select x+y as a, c, json_extract(payload, "$.meta.user") as user where b = 10
    * group_by user
    * agg - count(a, window=7d, bucket=c)
    *
    * Stage = SELECT, output = a,  expression = x + y, input_columns = [x, y], type = DIRECT
    *                              expression = "b = 10", input_columns = [b], type = WHERE
    *                 output=user, expression = 'json_extract(payload, "$.meta.user")', input_columns = [payload], type = GROUP_BY_KEY
    *
    * Stage = AGG,    output = a_count_7d_by_c, expression = 'count(a, window=7d, bucket=c)', input_columns = [a, c], type = DIRECT
    *                 output=user, input_columns = [user], type = GROUP_BY_KEY
    **/
enum ColumnLineageType {
    WHERE,
    JOIN_KEY,
    GROUP_BY_KEY,
    DERIVE,
    TIMESTAMP,
    DIRECT
}

struct ColumnLineage {
    1: optional list<Column> inputColumns
    2: optional string expression
    3: optional Column outputColumn
    4: optional ColumnLineageType lineageType // not present means direct
}

struct StageWithLineage {
    1: optional ColumnLineageStage stage
    2: optional list<ColumnLineage> lineage
}