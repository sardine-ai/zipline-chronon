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

import inspect
import json
import logging
from copy import deepcopy
from typing import Callable, Dict, List, Optional, Tuple, Union

import ai.chronon.api.common.ttypes as common
import ai.chronon.api.ttypes as ttypes
import ai.chronon.utils as utils
import ai.chronon.windows as window_utils

OperationType = int  # type(zthrift.Operation.FIRST)

#  The GroupBy's default online/production status is None and it will inherit
# online/production status from the Joins it is included.
# If it is included in multiple joins, it is considered online/production
# if any of the joins are online/production. Otherwise it is not online/production
# unless it is explicitly marked as online/production on the GroupBy itself.
DEFAULT_ONLINE = None
DEFAULT_PRODUCTION = None
LOGGER = logging.getLogger()


def collector(
    op: ttypes.Operation,
) -> Callable[[ttypes.Operation], Tuple[ttypes.Operation, Dict[str, str]]]:
    return lambda k: (op, {"k": str(k)})


def generic_collector(op: ttypes.Operation, required, **kwargs):
    def _collector(*args, **other_args):
        arguments = kwargs.copy() if kwargs else {}
        for idx, arg in enumerate(required):
            arguments[arg] = args[idx]
        arguments.update(other_args)
        return (op, {k: str(v) for k, v in arguments.items()})

    return _collector


# To simplify imports
class Accuracy(ttypes.Accuracy):
    pass


class Operation:

    MIN = ttypes.Operation.MIN
    """Minimum value in the column"""

    MAX = ttypes.Operation.MAX
    """Maximum value in the column"""

    FIRST = ttypes.Operation.FIRST
    """First non-null value of input column by time column"""

    LAST = ttypes.Operation.LAST
    """Last non-null value of input column by time column"""

    APPROX_UNIQUE_COUNT = ttypes.Operation.APPROX_UNIQUE_COUNT
    """Approximate count of unique values using CPC (Compressed Probability Counting) sketch"""

    APPROX_UNIQUE_COUNT_LGK = collector(ttypes.Operation.APPROX_UNIQUE_COUNT)
    """Configurable approximate unique count with lgK parameter for sketch size tuning.
    Default lgK is 8. See CpcSketch.java for accuracy vs size tradeoffs:
    https://github.com/apache/incubator-datasketches-java/blob/master/src/main/java/org/apache/datasketches/cpc/CpcSketch.java#L180
    """

    UNIQUE_COUNT = ttypes.Operation.UNIQUE_COUNT
    """
    Exact count of unique values of the input column.
    Will store the set of items and can be expensive if the cardinality of the column is high.
    """

    COUNT = ttypes.Operation.COUNT
    """Total count of non-null values of the input column"""

    SUM = ttypes.Operation.SUM
    """Sum of values in the input column"""

    AVERAGE = ttypes.Operation.AVERAGE
    """Arithmetic mean of values in the input column"""

    VARIANCE = ttypes.Operation.VARIANCE
    """Statistical variance of values in the input column"""

    SKEW = ttypes.Operation.SKEW
    """Skewness (third standardized moment) of the distribution of values in input column"""

    KURTOSIS = ttypes.Operation.KURTOSIS
    """Kurtosis (fourth standardized moment) of the distribution of values in input column"""

    HISTOGRAM = ttypes.Operation.HISTOGRAM
    """Full frequency distribution of values"""

    FREQUENT_K = collector(ttypes.Operation.HISTOGRAM)
    """
    !! Could be expensive if the cardinality of the column is high !!
    Computes columns values that are frequent in the input column exactly.
    Produces a map of items as keys and counts as values.
    """

    APPROX_FREQUENT_K = collector(ttypes.Operation.APPROX_FREQUENT_K)
    """
    Computes columns values that are frequent in the input column approximately.
    Produces a map of items as keys and counts as values approximately.
    """

    APPROX_HEAVY_HITTERS_K = collector(ttypes.Operation.APPROX_HEAVY_HITTERS_K)
    """
    Computes column values that are skewed in the input column.
    Produces a map of items as keys and counts as values approximately.
    Different from APPROX_FREQUENT_K in that it only retains if a value is abnormally
    more frequent.
    """

    FIRST_K = collector(ttypes.Operation.FIRST_K)
    """Returns first k input column values by time column"""

    LAST_K = collector(ttypes.Operation.LAST_K)
    """Returns last k input column values by time column"""

    TOP_K = collector(ttypes.Operation.TOP_K)
    """Returns k largest values of the input column. Input needs to be sortable."""

    BOTTOM_K = collector(ttypes.Operation.BOTTOM_K)
    """Returns k smallest values of the input column"""

    APPROX_PERCENTILE = generic_collector(
        ttypes.Operation.APPROX_PERCENTILE, ["percentiles"], k=20
    )
    """Approximate percentile calculation with configurable accuracy parameter k=20"""


def Aggregations(**agg_dict):
    assert all(isinstance(agg, ttypes.Aggregation) for agg in agg_dict.values())
    for key, agg in agg_dict.items():
        if not agg.inputColumn:
            agg.inputColumn = key
    return agg_dict.values()


def DefaultAggregation(keys, sources, operation=Operation.LAST, tags=None):
    aggregate_columns = []
    for source in sources:
        query = utils.get_query(source)
        columns = utils.get_columns(source)
        non_aggregate_columns = keys + [
            "ts",
            "is_before",
            "mutation_ts",
            "ds",
            query.timeColumn,
        ]
        aggregate_columns += [
            column for column in columns if column not in non_aggregate_columns
        ]
    return [
        Aggregation(operation=operation, input_column=column, tags=tags)
        for column in aggregate_columns
    ]


class TimeUnit:
    HOURS = common.TimeUnit.HOURS
    DAYS = common.TimeUnit.DAYS


def window_to_str_pretty(window: common.Window):
    unit = common.TimeUnit._VALUES_TO_NAMES[window.timeUnit].lower()
    return f"{window.length} {unit}"


def op_to_str(operation: OperationType):
    return ttypes.Operation._VALUES_TO_NAMES[operation].lower()


# See docs/Aggregations.md
def Aggregation(
    input_column: str = None,
    operation: Union[ttypes.Operation, Tuple[ttypes.Operation, Dict[str, str]]] = None,
    windows: Union[List[common.Window], List[str]] = None,
    buckets: List[str] = None,
    tags: Dict[str, str] = None,
) -> ttypes.Aggregation:
    """
    :param input_column:
        Column on which the aggregation needs to be performed.
        This should be one of the input columns specified on the keys of the `select` in the `Query`'s `Source`
    :type input_column: str
    :param operation:
        Operation to use to aggregate the input columns. For example, MAX, MIN, COUNT
        Some operations have arguments, like last_k, approx_percentiles etc.,
        Defaults to "LAST".
    :type operation: ttypes.Operation
    :param windows:
        Length to window to calculate the aggregates on. Strings like "1h", "30d" are also accepted.
        Minimum window size is 1hr. Maximum can be arbitrary. When not defined, the computation is un-windowed.
    :type windows: List[common.Window]
    :param buckets:
        Besides the GroupBy.keys, this is another level of keys for use under this aggregation.
        Using this would create an output as a map of string to aggregate.
    :type buckets: List[str]
    :return: An aggregate defined with the specified operation.
    """
    # Default to last
    operation = operation if operation is not None else Operation.LAST
    arg_map = {}
    if isinstance(operation, tuple):
        operation, arg_map = operation[0], operation[1]

    def normalize(w: Union[common.Window, str]) -> common.Window:
        if isinstance(w, str):
            return window_utils._from_str(w)
        elif isinstance(w, common.Window):
            return w
        else:
            raise Exception(
                "window should be either a string like '7d', '24h', or a Window type"
            )

    norm_windows = [normalize(w) for w in windows] if windows else None

    agg = ttypes.Aggregation(input_column, operation, arg_map, norm_windows, buckets)

    agg.tags = tags
    return agg


def Window(length: int, time_unit: common.TimeUnit) -> common.Window:
    return common.Window(length, time_unit)


def Derivation(name: str, expression: str) -> ttypes.Derivation:
    """
    Derivation allows arbitrary SQL select clauses to be computed using columns from the output of group by backfill
    output schema. It is supported for offline computations for now.

    If both name and expression are set to "*", then every raw column will be included along with the derived columns.

    :param name: output column name of the SQL expression
    :param expression: any valid Spark SQL select clause based on joinPart or externalPart columns
    :return: a Derivation object representing a single derived column or a wildcard ("*") selection.
    """
    return ttypes.Derivation(name=name, expression=expression)


def contains_windowed_aggregation(aggregations: Optional[List[ttypes.Aggregation]]):
    if not aggregations:
        return False
    for agg in aggregations:
        if agg.windows:
            return True
    return False


def validate_group_by(group_by: ttypes.GroupBy):
    sources = group_by.sources
    keys = group_by.keyColumns
    aggregations = group_by.aggregations
    # check ts is not included in query.select
    first_source_columns = set(utils.get_columns(sources[0]))
    # TODO undo this check after ml_models CI passes
    assert "ts" not in first_source_columns, (
        "'ts' is a reserved key word for Chronon,"
        " please specify the expression in timeColumn"
    )
    for src in sources:
        query = utils.get_query(src)
        if src.events:
            assert query.mutationTimeColumn is None, (
                "ingestionTimeColumn should not be specified for "
                "event source as it should be the same with timeColumn"
            )
            assert query.reversalColumn is None, (
                "reversalColumn should not be specified for event source "
                "as it won't have mutations"
            )
            if group_by.accuracy != Accuracy.SNAPSHOT:
                assert query.timeColumn is not None, (
                    "please specify query.timeColumn for non-snapshot accurate "
                    "group by with event source"
                )
        else:
            if contains_windowed_aggregation(aggregations):
                assert (
                    query.timeColumn
                ), "Please specify timeColumn for entity source with windowed aggregations"

    column_set = None
    # all sources should select the same columns
    for i, source in enumerate(sources[1:]):
        column_set = set(utils.get_columns(source))
        column_diff = column_set ^ first_source_columns
        assert not column_diff, f"""
Mismatched columns among sources [1, {i+2}], Difference: {column_diff}
"""

    # all keys should be present in the selected columns
    unselected_keys = set(keys) - first_source_columns
    assert not unselected_keys, f"""
Keys {unselected_keys}, are unselected in source
"""

    # Aggregations=None is only valid if group_by is Entities
    if aggregations is None:
        is_events = any([s.events for s in sources])
        has_mutations = (
            any(
                [
                    (
                        s.entities.mutationTable is not None
                        or s.entities.mutationTopic is not None
                    )
                    for s in sources
                    if s.entities is not None
                ]
            )
            if not is_events
            else False
        )
        assert not (
            is_events or has_mutations
        ), "You can only set aggregations=None in an EntitySource without mutations"
    else:
        columns = set([c for src in sources for c in utils.get_columns(src)])
        for agg in aggregations:
            assert agg.inputColumn, (
                f"input_column is required for all operations, found: input_column = {agg.inputColumn} "
                f"and operation {op_to_str(agg.operation)}"
            )
            assert (agg.inputColumn in columns) or (agg.inputColumn == "ts"), (
                f"input_column: for aggregation is not part of the query. Available columns: {column_set} "
                f"input_column: {agg.inputColumn}"
            )
            if agg.operation == ttypes.Operation.APPROX_PERCENTILE:
                if agg.argMap is not None and agg.argMap.get("percentiles") is not None:
                    try:
                        percentile_array = json.loads(agg.argMap["percentiles"])
                        assert isinstance(percentile_array, list)
                        assert all(
                            [float(p) >= 0 and float(p) <= 1 for p in percentile_array]
                        )
                    except Exception as e:
                        LOGGER.exception(e)
                        raise ValueError(
                            "[Percentiles] Unable to decode percentiles value, expected json array with values between"
                            f" 0 and 1 inclusive (ex: [0.6, 0.1]), received: {agg.argMap['percentiles']}"
                        ) from e
                else:
                    raise ValueError(
                        f"[Percentiles] Unsupported arguments for {op_to_str(agg.operation)}, "
                        "example required: {'k': '128', 'percentiles': '[0.4,0.5,0.95]'},"
                        f" received: {agg.argMap}\n"
                    )
            if agg.windows:
                assert not (
                    # Snapshot accuracy.
                    (
                        (group_by.accuracy and group_by.accuracy == Accuracy.SNAPSHOT)
                        or group_by.backfillStartDate
                    )
                    and
                    # Hourly aggregation.
                    any([window.timeUnit == TimeUnit.HOURS for window in agg.windows])
                ), (
                    "Detected a snapshot accuracy group by with an hourly aggregation. Resolution with snapshot "
                    "accuracy is not fine enough to allow hourly group bys. Consider removing the `backfill start "
                    "date` param if set or adjusting the aggregation window. "
                    f"input_column: {agg.inputColumn}, windows: {agg.windows}"
                )


_ANY_SOURCE_TYPE = Union[
    ttypes.Source, ttypes.EventSource, ttypes.EntitySource, ttypes.JoinSource
]


def _get_op_suffix(operation, argmap):
    op_str = op_to_str(operation)
    if operation in [
        ttypes.Operation.LAST_K,
        ttypes.Operation.TOP_K,
        ttypes.Operation.FIRST_K,
        ttypes.Operation.BOTTOM_K,
    ]:
        op_name_suffix = op_str[:-2]
        arg_suffix = argmap.get("k")
        return "{}{}".format(op_name_suffix, arg_suffix)
    else:
        return op_str


def get_output_col_names(aggregation):
    base_name = f"{aggregation.inputColumn}_{_get_op_suffix(aggregation.operation, aggregation.argMap)}"
    windowed_names = []
    if aggregation.windows:
        for window in aggregation.windows:
            unit = common.TimeUnit._VALUES_TO_NAMES[window.timeUnit].lower()[0]
            window_suffix = f"{window.length}{unit}"
            windowed_names.append(f"{base_name}_{window_suffix}")
    else:
        windowed_names = [base_name]

    bucketed_names = []
    if aggregation.buckets:
        for bucket in aggregation.buckets:
            bucketed_names.extend([f"{name}_by_{bucket}" for name in windowed_names])
    else:
        bucketed_names = windowed_names

    return bucketed_names


def GroupBy(
    sources: Union[List[_ANY_SOURCE_TYPE], _ANY_SOURCE_TYPE],
    keys: List[str],
    aggregations: Optional[List[ttypes.Aggregation]],
    derivations: List[ttypes.Derivation] = None,
    accuracy: ttypes.Accuracy = None,
    backfill_start_date: str = None,
    output_namespace: str = None,
    table_properties: Dict[str, str] = None,
    tags: Dict[str, str] = None,
    online: bool = DEFAULT_ONLINE,
    production: bool = DEFAULT_PRODUCTION,
    # execution params
    offline_schedule: str = "@daily",
    conf: common.ConfigProperties = None,
    env_vars: common.EnvironmentVariables = None,
    step_days: int = None,
    disable_historical_backfill: bool = False,
) -> ttypes.GroupBy:
    """

    :param sources:
        can be constructed as entities or events or joinSource::

            import ai.chronon.api.ttypes as chronon
            events = chronon.Source(events=chronon.Events(
                table=YOUR_TABLE,
                topic=YOUR_TOPIC #  <- OPTIONAL for serving
                query=chronon.Query(...)
                isCumulative=False  # <- defaults to false.
            ))
            Or
            entities = chronon.Source(entities=chronon.Entities(
                snapshotTable=YOUR_TABLE,
                mutationTopic=YOUR_TOPIC,
                mutationTable=YOUR_MUTATION_TABLE
                query=chronon.Query(...)
            ))
            or
            joinSource =  chronon.Source(joinSource=chronon.JoinSource(
                join = YOUR_CHRONON_PARENT_JOIN,
                query = chronon.Query(...)
            ))

        Multiple sources can be supplied to backfill the historical values with their respective start and end
        partitions. However, only one source is allowed to be a streaming one.
    :type sources: List[ai.chronon.api.ttypes.Events|ai.chronon.api.ttypes.Entities]
    :param keys:
        List of primary keys that defines the data that needs to be collected in the result table. Similar to the
        GroupBy in the SQL context.
    :type keys: List[String]
    :param aggregations:
        List of aggregations that needs to be computed for the data following the grouping defined by the keys::

            import ai.chronon.api.ttypes as chronon
            aggregations = [
                chronon.Aggregation(input_column="entity", operation=Operation.LAST),
                chronon.Aggregation(input_column="entity", operation=Operation.LAST, windows=['7d'])
            ],
    :type aggregations: List[ai.chronon.api.ttypes.Aggregation]
    :param online:
        Should we upload the result data of this conf into the KV store so that we can fetch/serve this GroupBy online.
        Once Online is set to True, you ideally should not change the conf.
    :type online: bool
    :param production:
        This when set can be integrated to trigger alerts. You will have to integrate this flag into your alerting
        system yourself.
    :type production: bool
    :param backfill_start_date:
        Start date from which GroupBy data should be computed. This will determine how back of a time that Chronon would
        goto to compute the resultant table and its aggregations.
    :type backfill_start_date: str
    :param env:
        This is a dictionary of "mode name" to dictionary of "env var name" to "env var value"::

            {
                'backfill' : { 'VAR1' : 'VAL1', 'VAR2' : 'VAL2' },
                'upload' : { 'VAR1' : 'VAL1', 'VAR2' : 'VAL2' }
                'streaming' : { 'VAR1' : 'VAL1', 'VAR2' : 'VAL2' }
            }

        These vars then flow into run.py and the underlying spark_submit.sh.
        These vars can be set in other places as well. The priority order (descending) is as below

        1. env vars set while using run.py "VAR=VAL run.py --mode=backfill <name>"
        2. env vars set here in Join's env param
        3. env vars set in `team.json['team.production.<MODE NAME>']`
        4. env vars set in `team.json['default.production.<MODE NAME>']`

    :type env: Dict[str, Dict[str, str]]
    :param table_properties:
        Specifies the properties on output hive tables. Can be specified in teams.json.
    :type table_properties: Dict[str, str]
    :param output_namespace:
        In backfill mode, we will produce data into hive. This represents the hive namespace that the data will be
        written into. You can set this at the teams.json level.
    :type output_namespace: str
    :param accuracy:
        Defines the computing accuracy of the GroupBy.
        If "Snapshot" is selected, the aggregations are computed based on the partition identifier - "ds" time column.
        If "Temporal" is selected, the aggregations are computed based on the event time - "ts" time column.
    :type accuracy: ai.chronon.api.ttypes.SNAPSHOT or ai.chronon.api.ttypes.TEMPORAL
    :param lag:
        Param that goes into customJson. You can pull this out of the json at path "metaData.customJson.lag"
        This is used by airflow integration to pick an older hive partition to wait on.
    :type lag: int
    :param offline_schedule:
        the offline schedule interval for batch jobs. Below is the equivalent of the cron tab commands::

            '@hourly': '0 * * * *',
            '@daily': '0 0 * * *',
            '@weekly': '0 0 * * 0',
            '@monthly': '0 0 1 * *',
            '@yearly': '0 0 1 1 *',

    :type offline_schedule: str
    :param tags:
        Additional metadata that does not directly affect feature computation, but is useful to
        track for management purposes.
    :type tags: Dict[str, str]
    :param derivations:
        Derivation allows arbitrary SQL select clauses to be computed using columns from the output of group by backfill
        output schema. It is supported for offline computations for now.
    :type derivations: List[ai.chronon.api.ttypes.Drivation]
    :param kwargs:
        Additional properties that would be passed to run.py if specified under additional_args property.
        And provides an option to pass custom values to the processing logic.
    :type kwargs: Dict[str, str]
    :param conf:
        Configuration properties for the GroupBy. Depending on the mode we layer confs with the following priority:
        1. conf set in the GroupBy.conf.<mode>
        2. conf set in the GroupBy.conf.common
        3. conf set in the team.conf.<mode>
        4. conf set in the team.conf.common
        5. conf set in the default.conf.<mode>
        6. conf set in the default.conf.common
    :param env_vars:
        Environment variables for the GroupBy. Depending on the mode we layer envs with the following priority:
        1. env vars set in the GroupBy.env.<mode>
        2. env vars set in the GroupBy.env.common
        3. env vars set in the team.env.<mode>
        4. env vars set in the team.env.common
        5. env vars set in the default.env.<mode>
        6. env vars set in the default.env.common
    :param step_days
        The maximum number of days to output at once
    :return:
        A GroupBy object containing specified aggregations.
    """
    assert sources, "Sources are not specified"

    agg_inputs = []
    if aggregations is not None:
        agg_inputs = [agg.inputColumn for agg in aggregations]

    required_columns = keys + agg_inputs

    def _sanitize_columns(src: ttypes.Source):
        source = deepcopy(src)
        query = (
            source.entities.query
            if source.entities is not None
            else (
                source.events.query
                if source.events is not None
                else source.joinSource.query
            )
        )

        if query.selects is None:
            query.selects = {}
        for col in required_columns:
            if col not in query.selects:
                query.selects[col] = col
        if "ts" in query.selects:  # ts cannot be in selects.
            ts = query.selects["ts"]
            del query.selects["ts"]
            if query.timeColumn is None:
                query.timeColumn = ts
            assert query.timeColumn == ts, (
                f"mismatched `ts`: {ts} and `timeColumn`: {query.timeColumn} "
                "in source {source}. Please specify only the `timeColumn`"
            )
        return source

    def _normalize_source(source):
        if isinstance(source, ttypes.EventSource):
            return ttypes.Source(events=source)
        elif isinstance(source, ttypes.EntitySource):
            return ttypes.Source(entities=source)
        elif isinstance(source, ttypes.JoinSource):
            if not source.join.metadata.isSetOutputNamespace():
                source.join.metadata.setOutputNamespace(output_namespace)
            return ttypes.Source(joinSource=source)
        elif isinstance(source, ttypes.Source):
            return source
        else:
            print("unrecognized " + str(source))

    if not isinstance(sources, list):
        sources = [sources]

    sources = [_sanitize_columns(_normalize_source(source)) for source in sources]

    # get caller's filename to assign team
    team = inspect.stack()[1].filename.split("/")[-2]

    exec_info = common.ExecutionInfo(
        scheduleCron=offline_schedule,
        conf=conf,
        env=env_vars,
        stepDays=step_days,
        historicalBackfill=disable_historical_backfill,
    )

    column_tags = {}
    if aggregations:
        for agg in aggregations:
            if hasattr(agg, "tags") and agg.tags:
                for output_col in get_output_col_names(agg):
                    column_tags[output_col] = agg.tags


    metadata = ttypes.MetaData(
        online=online,
        production=production,
        outputNamespace=output_namespace,
        tableProperties=table_properties,
        team=team,
        executionInfo=exec_info,
        tags=tags if tags else None,
        columnTags=column_tags if column_tags else None,
    )

    group_by = ttypes.GroupBy(
        sources=sources,
        keyColumns=keys,
        aggregations=aggregations,
        metaData=metadata,
        backfillStartDate=backfill_start_date,
        accuracy=accuracy,
        derivations=derivations,
    )
    validate_group_by(group_by)

    return group_by
