
import inspect
import json
from dataclasses import dataclass
from typing import Dict, List, Optional

import ai.chronon.airflow_helpers as airflow_helpers
import ai.chronon.api.common.ttypes as common
import ai.chronon.api.ttypes as ttypes
from ai.chronon.constants import AIRFLOW_DEPENDENCIES_KEY


# Wrapper for EngineType
class EngineType:
    SPARK = ttypes.EngineType.SPARK
    BIGQUERY = ttypes.EngineType.BIGQUERY

@dataclass
class TableDependency:
    table: str
    partition_column: Optional[str] = None
    additional_partitions: Optional[List[str]] = None
    offset: Optional[int] = None
    partition_format: Optional[str] = None

def StagingQuery(
    name: str,
    query: str,
    output_namespace: Optional[str] = None,
    start_partition: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    setups: Optional[List[str]] = None,
    partition_column: Optional[str] = None,
    engine_type: Optional[EngineType] = None,
    dependencies: Optional[List[TableDependency]] = None,
    tags: Optional[Dict[str, str]] = None,
    # execution params
    offline_schedule: str = "@daily",
    conf: Optional[common.ConfigProperties] = None,
    env_vars: Optional[common.EnvironmentVariables] = None,
    step_days: Optional[int] = None,
) -> ttypes.StagingQuery:
    """
    Creates a StagingQuery object for executing arbitrary SQL queries with templated date parameters.

    :param query:
        Arbitrary spark query that should be written with template parameters:
        - `{{ start_date }}`: Initial run uses start_partition, future runs use latest partition + 1 day
        - `{{ end_date }}`: The end partition of the computing range
        - `{{ latest_date }}`: End partition independent of the computing range (for cumulative sources)
        - `{{ max_date(table=namespace.my_table) }}`: Max partition available for a given table
        These parameters can be modified with offset and bounds:
        - `{{ start_date(offset=-10, lower_bound='2023-01-01', upper_bound='2024-01-01') }}`
    :type query: str
    :param start_partition:
        On the first run, `{{ start_date }}` will be set to this user provided start date,
        future incremental runs will set it to the latest existing partition + 1 day.
    :type start_partition: str
    :param setups:
        Spark SQL setup statements. Used typically to register UDFs.
    :type setups: List[str]
    :param partition_column:
        Only needed for `max_date` template
    :type partition_column: str
    :param engine_type:
        By default, spark is the compute engine. You can specify an override (eg. bigquery, etc.)
        Use the EngineType class constants: EngineType.SPARK, EngineType.BIGQUERY, etc.
    :type engine_type: int
    :param tags:
        Additional metadata that does not directly affect computation, but is useful for management.
    :type tags: Dict[str, str]
    :param offline_schedule:
        The offline schedule interval for batch jobs. Format examples:
        '@hourly': '0 * * * *',
        '@daily': '0 0 * * *',
        '@weekly': '0 0 * * 0',
        '@monthly': '0 0 1 * *',
        '@yearly': '0 0 1 1 *'
    :type offline_schedule: str
    :param conf:
        Configuration properties for the StagingQuery.
    :type conf: common.ConfigProperties
    :param env_vars:
        Environment variables for the StagingQuery.
    :type env_vars: common.EnvironmentVariables
    :param step_days:
        The maximum number of days to process at once
    :type step_days: int
    :return:
        A StagingQuery object
    """
    # Get caller's filename to assign team
    team = inspect.stack()[1].filename.split("/")[-2]

    # Create execution info
    exec_info = common.ExecutionInfo(
        scheduleCron=offline_schedule,
        conf=conf,
        env=env_vars,
        stepDays=step_days,
    )

    airflow_dependencies = [airflow_helpers.create_airflow_dependency(t.table, t.partition_column, t.additional_partitions, t.offset, t.partition_format) for t in dependencies] if dependencies else []
    custom_json = json.dumps({AIRFLOW_DEPENDENCIES_KEY: airflow_dependencies})

    # Create metadata
    meta_data = ttypes.MetaData(
        name=name,
        outputNamespace=output_namespace,
        team=team,
        executionInfo=exec_info,
        tags=tags,
        customJson=custom_json,
        tableProperties=table_properties,
    )

    # Create and return the StagingQuery object with camelCase parameter names
    staging_query = ttypes.StagingQuery(
        metaData=meta_data,
        query=query,
        startPartition=start_partition,
        setups=setups,
        partitionColumn=partition_column,
        engineType=engine_type,
    )

    return staging_query