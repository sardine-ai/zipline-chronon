from joins.gcp import training_set

from ai.chronon.staging_query import StagingQuery, TableDependency
from ai.chronon.utils import get_join_output_table_name, get_staging_query_output_table_name

query = f"""
SELECT
    *
FROM {get_join_output_table_name(training_set.v1_test, True)}
WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
"""

def get_staging_query():
    return StagingQuery(
        query=query,
        start_partition="2020-03-01",
        name="sample_staging_query",
        output_namespace="data",
        table_properties={"sample_config_json": """{"sample_key": "sample value"}"""},
        dependencies=[
            TableDependency(table=get_join_output_table_name(training_set.v1_test, True), partition_column="ds", offset=1)
        ],
        version=0,
    )

v1 = get_staging_query()
v2 = get_staging_query()
v3 = get_staging_query()
v4 = get_staging_query()
v5 = get_staging_query()
v6 = get_staging_query()

terminal_v1 = StagingQuery(
    query=query,
    start_partition="2020-03-01",
    table_properties={"sample_config_json": """{"sample_key": "sample value"}"""},
    name="terminal_staging_query",
    output_namespace="data",
    dependencies=[
        TableDependency(table=get_staging_query_output_table_name(v1, True), partition_column="ds", offset=1),
        TableDependency(table=get_staging_query_output_table_name(v2, True), partition_column="ds", offset=1),
        TableDependency(table=get_staging_query_output_table_name(v3, True), partition_column="ds", offset=1),
        TableDependency(table=get_staging_query_output_table_name(v4, True), partition_column="ds", offset=1),
        TableDependency(table=get_staging_query_output_table_name(v5, True), partition_column="ds", offset=1),
        TableDependency(table=get_staging_query_output_table_name(v6, True), partition_column="ds", offset=1),
    ],
    version=0,
)

query_hub = f"""
SELECT
    *
FROM {get_join_output_table_name(training_set.v1_hub, True)}
WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
"""

v1_hub = StagingQuery(
    query=query_hub,
    start_partition="2020-03-01",
    name="sample_staging_query",
    output_namespace="data",
    table_properties={"sample_config_json": """{"sample_key": "sample value"}"""},
    dependencies=[
        TableDependency(table=get_join_output_table_name(training_set.v1_hub, True), partition_column="ds", offset=1)
    ],
    version=0,
)
