from ai.chronon.staging_query import EngineType, StagingQuery, TableDependency


def get_select_star_export(table: str, partition_column: str = "_PARTITIONTIME"):
    bigquery_export_sql = f"""
    SELECT 
        * 
    FROM demo.`{table}`
    WHERE 
    TIMESTAMP_TRUNC({partition_column}, DAY) BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """


    return StagingQuery(
        query=bigquery_export_sql,
        output_namespace="data",
        engine_type=EngineType.BIGQUERY,
        dependencies=[
            TableDependency(table=f"demo.`{table}`", partition_column=partition_column, offset=0)
        ],
        version=0,
    )




user_activities = StagingQuery(
    query="""
    SELECT 
        *,
        TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) as ds 
    FROM demo.`user-activities`
    WHERE 
    TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    """,
    output_namespace="data",
    engine_type=EngineType.BIGQUERY,
    dependencies=[
        TableDependency(table=f"demo.`user-activities`", partition_column="_PARTITIONTIME", offset=0)
    ],
    version=0,
)
checkouts = get_select_star_export("checkouts")
dim_listings = get_select_star_export("dim_listings", "ds")
dim_merchants = get_select_star_export("dim_merchants", "ds")
dim_users = get_select_star_export("dim_users", "ds")
