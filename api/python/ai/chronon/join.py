#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import copy
import gc
import importlib
import logging
from collections import Counter
from typing import Dict, List, Tuple

import ai.chronon.api.common.ttypes as common
import ai.chronon.api.ttypes as api
import ai.chronon.repo.extract_objects as eo
import ai.chronon.utils as utils

logging.basicConfig(level=logging.INFO)


def JoinPart(
    group_by: api.GroupBy,
    key_mapping: Dict[str, str] = None,
    prefix: str = None,
    tags: Dict[str, str] = None,
) -> api.JoinPart:
    """
    Specifies HOW to join the `left` of a Join with GroupBy's.

    :param group_by:
        The GroupBy object to join with. Keys on left are used to equi join with keys on right.
        When left is entities all GroupBy's are computed as of midnight.
        When left is events, we do a point-in-time join when right.accuracy == TEMPORAL OR right.source.topic != null
    :type group_by: ai.chronon.api.GroupBy
    :param key_mapping:
        Names of keys don't always match on left and right, this mapping tells us how to map when they don't.
    :type key_mapping: Dict[str, str]
    :param prefix:
        All the output columns of the groupBy will be prefixed with this string. This is used when you need to join
        the same groupBy more than once with `left`. Say on the left you have seller and buyer, on the group you have
        a user's avg_price, and you want to join the left (seller, buyer) with (seller_avg_price, buyer_avg_price) you
        would use key_mapping and prefix parameters.
    :param tags:
        Additional metadata about the JoinPart that you wish to track. Does not effect computation.
    :type tags: Dict[str, str]
    :return:
        JoinPart specifies how the left side of a join, or the query in online setting, would join with the right side
        components like GroupBys.
    """

    assert isinstance(
        group_by, api.GroupBy
    ), f"Expecting GroupBy. But found {type(group_by).__name__}"

    # used for reset for next run
    import_copy = __builtins__["__import__"]
    # get group_by's module info from garbage collector
    gc.collect()

    group_by_module_name = None
    for ref in gc.get_referrers(group_by):
        if (
            isinstance(
                ref, dict
            )  # Attaching methods to GroupBy adds references in GC, need to filter out
            and "__name__" in ref
            and ref["__name__"].startswith("group_bys")
        ):
            group_by_module_name = ref["__name__"]
            break

    if group_by_module_name:
        logging.debug(
            "group_by's module info from garbage collector {}".format(
                group_by_module_name
            )
        )
        group_by_module = importlib.import_module(group_by_module_name)
        __builtins__["__import__"] = eo.import_module_set_name(
            group_by_module, api.GroupBy
        )
    else:
        if not group_by.metaData.name:
            logging.error("No group_by file or custom group_by name found")
            raise ValueError(
                "[GroupBy] Must specify a group_by name if group_by is not defined in separate file. "
                "You may pass it in via GroupBy.name. \n"
            )

    if key_mapping:
        utils.check_contains(
            key_mapping.values(), group_by.keyColumns, "key", group_by.metaData.name
        )

    join_part = api.JoinPart(groupBy=group_by, keyMapping=key_mapping, prefix=prefix)
    join_part.tags = tags
    # reset before next run
    __builtins__["__import__"] = import_copy
    return join_part


FieldsType = List[Tuple[str, api.TDataType]]


class DataType:
    """
    Helper class to generate data types for declaring schema.
    This supports primitive like numerics, string etc., and complex
    types like Map, List, Struct etc.
    """

    BOOLEAN = api.TDataType(api.DataKind.BOOLEAN)
    SHORT = api.TDataType(api.DataKind.SHORT)
    INT = api.TDataType(api.DataKind.INT)
    LONG = api.TDataType(api.DataKind.LONG)
    FLOAT = api.TDataType(api.DataKind.FLOAT)
    DOUBLE = api.TDataType(api.DataKind.DOUBLE)
    STRING = api.TDataType(api.DataKind.STRING)
    BINARY = api.TDataType(api.DataKind.BINARY)

    # Types unsupported by Avro. See AvroConversions.scala#fromChrononSchema
    # BYTE = api.TDataType(api.DataKind.BYTE)
    # DATE = api.TDataType(api.DataKind.DATE)
    # TIMESTAMP = api.TDataType(api.DataKind.TIMESTAMP)

    def MAP(key_type: api.TDataType, value_type: api.TDataType) -> api.TDataType:
        assert key_type == api.TDataType(
            api.DataKind.STRING
        ), "key_type has to STRING for MAP types"

        return api.TDataType(
            api.DataKind.MAP,
            params=[api.DataField("key", key_type), api.DataField("value", value_type)],
        )

    def LIST(elem_type: api.TDataType) -> api.TDataType:
        return api.TDataType(
            api.DataKind.LIST, params=[api.DataField("elem", elem_type)]
        )

    def STRUCT(name: str, *fields: FieldsType) -> api.TDataType:
        return api.TDataType(
            api.DataKind.STRUCT,
            params=[api.DataField(name, data_type) for (name, data_type) in fields],
            name=name,
        )


def ExternalSource(
    name: str,
    team: str,
    key_fields: FieldsType,
    value_fields: FieldsType,
) -> api.ExternalSource:
    """
    External sources are online only data sources. During fetching, using
    chronon java client, they consume a Request containing a key map
    (name string to value). And produce a Response containing a value map.

    This is primarily used in Joins. We also expose a fetchExternal method in
    java client library that can be used to fetch a batch of External source
    requests efficiently.

    Internally Chronon will batch these requests to the service and parallelize
    fetching from different services, while de-duplicating given a batch of
    join requests.

    The implementation of how to fetch is an `ExternalSourceHandler` in
    scala/java api that needs to be registered while implementing
    ai.chronon.online.Api with the name used in the ExternalSource. This is
    meant for re-usability of external source definitions.

    :param name: name of the external source to fetch from. Should match
                 the name in the registry.
    :param key_fields: List of tuples of string and DataType. This is what
        will be given to ExternalSource handler registered in Java API.
        Eg., `[('key1', DataType.INT, 'key2', DataType.STRING)]`
    :param value_fields: List of tuples of string and DataType. This is what
        the ExternalSource handler will respond with::

            [
                ('value0', DataType.INT),
                ('value1', DataType.MAP(DataType.STRING, DataType.LONG),
                ('value2', DataType.STRUCT(
                    name = 'Context',
                    ('field1', DataType.INT),
                    ('field2', DataType.DOUBLE)
                ))
            ]

    """
    assert name != "contextual", "Please use `ContextualSource`"
    return api.ExternalSource(
        metadata=api.MetaData(name=name, team=team),
        keySchema=DataType.STRUCT(f"ext_{name}_keys", *key_fields),
        valueSchema=DataType.STRUCT(f"ext_{name}_values", *value_fields),
    )


def ContextualSource(fields: FieldsType, team="default") -> api.ExternalSource:
    """
    Contextual source values are passed along for logging. No external request is
    actually made.
    """
    return api.ExternalSource(
        metadata=api.MetaData(name="contextual", team=team),
        keySchema=DataType.STRUCT("contextual_keys", *fields),
        valueSchema=DataType.STRUCT("contextual_values", *fields),
    )


def ExternalPart(
    source: api.ExternalSource, key_mapping: Dict[str, str] = None, prefix: str = None
) -> api.ExternalPart:
    """
    Used to describe which ExternalSources to pull features from while fetching
    online. This data also goes into logs based on sample percent.

    Just as in JoinPart, key_mapping is used to map the join left's keys to
    external source's keys. "vendor" and "buyer" on left side (query map)
    could both map to a "user" in an account data external source. You would
    create one ExternalPart for vendor with params:
    `(key_mapping={vendor: user}, prefix=vendor)`
    another for buyer.

    This doesn't have any implications offline besides logging. "right_parts"
    can be both backfilled and logged. Whereas, "external_parts" can only be
    logged. If you need the ability to backfill an external source, look into
    creating an EntitySource with mutation data for point-in-time-correctness.

    :param source: External source to join with
    :param key_mapping: How to map the keys from the query/left side to the
                        source
    :param prefix: Sometime you want to use the same source to fetch data for
                   different entities in the query. Eg., A transaction
                   between a buyer and a seller might query "user information"
                   serivce/source that has information about both buyer &
                   seller
    """
    return api.ExternalPart(source=source, keyMapping=key_mapping, prefix=prefix)


def LabelParts(
    labels: List[api.JoinPart],
    left_start_offset: int,
    left_end_offset: int,
    label_offline_schedule: str = "@daily",
) -> api.LabelParts:
    """
    Used to describe labels in join. Label part can be viewed as regular join part but represent
    label data instead of regular feature data. Once labels are mature, label join job would join
    labels with features in the training window user specified within the label GroupBy-s.

    Since label join job will run continuously based on the schedule, multiple labels could be generated but with
    different label_ds or label version. Label join job would have all computed label versions available, as well as
    a view of latest version for easy label retrieval.

    LabelParts definition can be updated along the way, but label join job can only accommodate these changes going
    forward unless a backfill is manually triggered.

    Label aggregation is also supported but with conditions applied. Single aggregation with one window is allowed
    for now. If aggregation is present, we would infer the left_start_offset and left_end_offset same as window size
    and the param input will be ignored.

    :param labels: List of labels
    :param label_offline_schedule: Cron expression for Airflow to schedule a DAG for offline
                                   label join compute tasks
    """

    exec_info = common.ExecutionInfo(
        scheduleCron=label_offline_schedule,
    )
    label_metadata = api.MetaData(executionInfo=exec_info)

    return api.LabelParts(
        labels=labels,
        metaData=label_metadata,
    )


def Derivation(name: str, expression: str) -> api.Derivation:
    """
    Derivation allows arbitrary SQL select clauses to be computed using columns from joinPart and externalParts,
    and saves the result as derived columns. The results will be available both in online fetching response map,
    and in offline Hive table.

    joinPart column names are automatically constructed according to the below convention
    `{join_part_prefix}_{group_by_name}_{input_column_name}_{aggregation_operation}_{window}_{by_bucket}`
    prefix, window and bucket are optional. You can find the type information of columns using the analyzer tool.

    externalPart column names are automatically constructed according to the below convention
    `ext_{external_source_name}_{value_column}`.
    Types are defined along with the schema by users for external sources.

    Note that only values can be used in derivations, not keys. If you want to use a key in the derivation, you must
    define it as a contextual field. You also must refer to a contextual field with its prefix included, for example:
    `ext_contextual_request_id`.

    If both name and expression are set to "*", then every raw column will be included along with the derived columns.

    :param name: output column name of the SQL expression
    :param expression: any valid Spark SQL select clause based on joinPart or externalPart columns
    :return: a Derivation object representing a single derived column or a wildcard ("*") selection.
    """
    return api.Derivation(name=name, expression=expression)


def BootstrapPart(
    table: str, key_columns: List[str] = None, query: api.Query = None
) -> api.BootstrapPart:
    """
    Bootstrap is the concept of using pre-computed feature values and skipping backfill computation during the
    training data generation phase. Bootstrap can be used for many purposes:
    - Generating ongoing feature values from logs
    - Backfilling feature values for external features (in which case Chronon is unable to run backfill)
    - Initializing a new Join by migrating old data from an older Join and reusing data

    One can bootstrap against any of these:

    - join part fields:
        Bootstrap can happen at individual field level within a join part.
        If all fields within a group by are bootstrapped, then we skip computation for group by. Otherwise, the whole
        thing will be re-run but only the values for the non-bootstrapped fields will be retained in the final table.
    - external part fields:
        Bootstrap can happen at individual field level within an external part.
        Since there is no backfill logic in chronon for external part, all non-bootstrapped fields in external parts
        are left as NULLs.
    - derivation fields:
        Derived fields can also be bootstrapped. Since derived fields depend on "base" fields (either join part or
        external part), chronon will try to trigger the least amount of computation possible. For example,
        if there is a join part where all derived fields that depend on the join part have been bootstrapped,
        then we skip the computation for this join part.
    - keys:
        Keys of both join parts and external parts can be bootstrapped. During offline table generation, we will first
        try to utilize key's data from left table; if it's not there, then we utilize bootstrap.
        For contextual features, we also support propagating the key bootstrap to the values.


    :param table: Name of hive table that contains feature values where rows are 1:1 mapped to left table
    :param key_columns: Keys to join bootstrap table to left table
    :param query: Selected columns (features & keys) and filtering conditions of the bootstrap tables.
    """
    return api.BootstrapPart(table=table, query=query, keyColumns=key_columns)


def Join(
    left: api.Source,
    right_parts: List[api.JoinPart],
    online_external_parts: List[api.ExternalPart] = None,
    bootstrap_parts: List[api.BootstrapPart] = None,
    bootstrap_from_log: bool = False,
    row_ids: List[str] = None,
    skew_keys: Dict[str, List[str]] = None,
    derivations: List[api.Derivation] = None,
    label_part: api.LabelParts = None,
    output_namespace: str = None,
    table_properties: Dict[str, str] = None,
    online: bool = False,
    production: bool = False,
    sample_percent: float = 100.0,
    check_consistency: bool = None,
    consistency_sample_percent: float = 5.0,
    use_long_names: bool = False,
    # execution params
    offline_schedule: str = "@daily",
    historical_backfill: bool = None,
    conf: common.ConfigProperties = None,
    env_vars: common.EnvironmentVariables = None,
    cluster_conf: common.ClusterConfigProperties = None,
    step_days: int = None,
) -> api.Join:
    """
    Construct a join object. A join can pull together data from various GroupBy's both offline and online. This is also
    the focal point for logging, data quality computation and monitoring. A join maps 1:1 to models in ML usage.

    :param left:
        The source on the left side, when Entities, all GroupBys are join with SNAPSHOT accuracy (midnight values).
        When left is events, if on the right, either when GroupBy's are TEMPORAL, or when topic is specified, we perform
        a TEMPORAL / point-in-time join.
    :type left: ai.chronon.api.Source
    :param right_parts:
        The list of groupBy's to join with. GroupBy's are wrapped in a JoinPart, which contains additional information
        on how to join the left side with the GroupBy.
    :type right_parts: List[ai.chronon.api.JoinPart]
    :param check_consistency:
        If online serving data should be compared with backfill data - as online-offline-consistency metrics.
        The metrics go into hive and your configured kv store for further visualization and monitoring.
    :type check_consistency: bool
    :param additional_args:
        Additional args go into `customJson` of `ai.chronon.api.MetaData` within the `ai.chronon.api.Join` object.
        This is a place for arbitrary information you want to tag your conf with.
    :type additional_args: List[str]
    :param additional_env:
        Deprecated, see env
    :type additional_env: List[str]
    :param online:
        Should we upload this conf into kv store so that we can fetch/serve this join online.
        Once Online is set to True, you ideally should not change the conf.
    :type online: bool
    :param production:
        This when set can be integrated to trigger alerts. You will have to integrate this flag into your alerting
        system yourself.
    :type production: bool
    :param output_namespace:
        In backfill mode, we will produce data into hive. This represents the hive namespace that the data will be
        written into. You can set this at the teams.json level.
    :type output_namespace: str
    :param table_properties:
        Specifies the properties on output hive tables. Can be specified in teams.json.
    :param lag:
        Param that goes into customJson. You can pull this out of the json at path "metaData.customJson.lag"
        This is used by airflow integration to pick an older hive partition to wait on.
    :param skew_keys:
        While back-filling, if there are known irrelevant keys - like user_id = 0 / NULL etc. You can specify them here.
        This is used to blacklist crawlers etc
    :param sample_percent:
        Online only parameter. What percent of online serving requests to this join should be logged into warehouse.
    :param consistency_sample_percent:
        Online only parameter. What percent of online serving requests to this join should be sampled to compute
        online offline consistency metrics.
        if sample_percent=50.0 and consistency_sample_percent=10.0, then basically the consistency job runs on
        5% of total traffic.
    :param online_external_parts:
        users can register external sources into Api implementation. Chronon fetcher can invoke the implementation.
        This is applicable only for online fetching. Offline this will not be produce any values.
    :param offline_schedule:
        Cron expression for Airflow to schedule a DAG for offline join compute tasks
    :param row_ids:
        Columns of the left table that uniquely define a training record. Used as default keys during bootstrap
    :param bootstrap_parts:
        A list of BootstrapPart used for the Join. See BootstrapPart doc for more details
    :param bootstrap_from_log:
        If set to True, will use logging table to generate training data by default and skip continuous backfill.
        Logging will be treated as another bootstrap source, but other bootstrap_parts will take precedence.
    :param label_part:
        Label part which contains a list of labels and label refresh window boundary used for the Join
    :param historical_backfill:
        Flag to indicate whether join backfill should backfill previous holes.
        Setting to false will only backfill latest single partition
    :type historical_backfill: bool
    :return:
        A join object that can be used to backfill or serve data. For ML use-cases this should map 1:1 to model.
    :param conf:
        Configuration properties for the join. Depending on the mode we layer confs with the following priority:
        1. conf set in the join.conf.<mode>
        2. conf set in the join.conf.common
        3. conf set in the team.conf.<mode>
        4. conf set in the team.conf.common
        5. conf set in the default.conf.<mode>
        6. conf set in the default.conf.common
    :param env_vars:
        Environment variables for the join. Depending on the mode we layer envs with the following priority:
        1. env vars set in the join.env.<mode>
        2. env vars set in the join.env.common
        3. env vars set in the team.env.<mode>
        4. env vars set in the team.env.common
        5. env vars set in the default.env.<mode>
        6. env vars set in the default.env.common
    :param cluster_conf:
        Cluster configuration properties for the join.
    :param step_days:
        The maximum number of days to output at once
    """
    # create a deep copy for case: multiple LeftOuterJoin use the same left,
    # validation will fail after the first iteration
    updated_left = copy.deepcopy(left)
    if left.events and left.events.query.selects:
        assert "ts" not in left.events.query.selects.keys(), (
            "'ts' is a reserved key word for Chronon,"
            " please specify the expression in timeColumn"
        )
        # mapping ts to query.timeColumn to events only
        updated_left.events.query.selects.update(
            {"ts": updated_left.events.query.timeColumn}
        )

    if label_part:
        label_metadata = api.MetaData(
            executionInfo=label_part.metaData.executionInfo,
        )
        label_part = api.LabelParts(
            labels=label_part.labels,
            leftStartOffset=label_part.leftStartOffset,
            leftEndOffset=label_part.leftEndOffset,
            metaData=label_metadata,
        )

    consistency_sample_percent = (
        consistency_sample_percent if check_consistency else None
    )

    # external parts need to be unique on (prefix, part.source.metaData.name)
    if online_external_parts:
        count_map = Counter(
            [(part.prefix, part.source.metadata.name) for part in online_external_parts]
        )
        has_duplicates = False
        for key, count in count_map.items():
            if count > 1:
                has_duplicates = True
                print(f"Found {count - 1} duplicate(s) for external part {key}")
        assert (
            has_duplicates is False
        ), "Please address all the above mentioned duplicates."

    if bootstrap_from_log:
        has_logging = sample_percent > 0 and online
        assert (
            has_logging
        ), "Join must be online with sample_percent set in order to use bootstrap_from_log option"
        bootstrap_parts = (bootstrap_parts or []) + [
            api.BootstrapPart(
                # templated values will be replaced when metaData.name is set at the end
                table="{{ logged_table }}"
            )
        ]

    exec_info = common.ExecutionInfo(
        scheduleCron=offline_schedule,
        conf=conf,
        env=env_vars,
        stepDays=step_days,
        historicalBackfill=historical_backfill,
        clusterConf=cluster_conf
    )

    metadata = api.MetaData(
        online=online,
        production=production,
        outputNamespace=output_namespace,
        tableProperties=table_properties,
        samplePercent=sample_percent,
        consistencyCheck=check_consistency,
        consistencySamplePercent=consistency_sample_percent,
        executionInfo=exec_info,
    )

    join = api.Join(
        left=updated_left,
        joinParts=right_parts,
        metaData=metadata,
        skewKeys=skew_keys,
        onlineExternalParts=online_external_parts,
        bootstrapParts=bootstrap_parts,
        rowIds=row_ids,
        labelParts=label_part,
        derivations=derivations,
        useLongNames=use_long_names,
    )

    return join
