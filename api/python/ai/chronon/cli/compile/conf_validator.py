"""Object for checking whether a Chronon API thrift object is consistent with other
"""

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

import json
import logging
import re
import textwrap
from collections import defaultdict
from typing import Dict, List, Set, Tuple

import ai.chronon.api.common.ttypes as common
from ai.chronon.api.ttypes import (
    Accuracy,
    Aggregation,
    Derivation,
    ExternalPart,
    GroupBy,
    Join,
    JoinPart,
    Source,
)
from ai.chronon.group_by import get_output_col_names
from ai.chronon.logger import get_logger
from ai.chronon.repo.serializer import thrift_simple_json

# Fields that indicate status of the entities.
SKIPPED_FIELDS = frozenset(["metaData"])
EXTERNAL_KEY = "onlineExternalParts"


def _filter_skipped_fields_from_join(json_obj: Dict, skipped_fields):
    for join_part in json_obj["joinParts"]:
        group_by = join_part["groupBy"]
        for field in skipped_fields:
            group_by.pop(field, None)
    if EXTERNAL_KEY in json_obj:
        json_obj.pop(EXTERNAL_KEY, None)


def _is_batch_upload_needed(group_by: GroupBy) -> bool:
    if group_by.metaData.online or group_by.backfillStartDate:
        return True
    else:
        return False


def is_identifier(s: str) -> bool:
    identifier_regex = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")
    return re.fullmatch(identifier_regex, s) is not None


def get_pre_derived_group_by_features(group_by: GroupBy) -> List[str]:
    output_columns = []
    # For group_bys with aggregations, aggregated columns
    if group_by.aggregations:
        for agg in group_by.aggregations:
            output_columns.extend(get_output_col_names(agg))
    # For group_bys without aggregations, selected fields from query
    else:
        for source in group_by.sources:
            output_columns.extend(get_pre_derived_source_keys(source))
    return output_columns


def get_pre_derived_group_by_columns(group_by: GroupBy) -> List[str]:
    output_columns = get_pre_derived_group_by_features(group_by)
    output_columns.extend(group_by.keyColumns)
    return output_columns


def get_group_by_output_columns(group_by: GroupBy, exclude_keys: bool = False) -> List[str]:
    """
    From the group_by object, get the final output columns after derivations.
    """
    all_output_columns = set(get_pre_derived_group_by_columns(group_by))
    if exclude_keys:
        output_columns = all_output_columns - set(group_by.keyColumns)
    else:
        output_columns = all_output_columns
    if group_by.derivations:
        return build_derived_columns(output_columns, group_by.derivations)
    else:
        return list(output_columns)


def get_pre_derived_join_internal_features(join: Join) -> List[str]:
    internal_features = []
    for jp in join.joinParts:
        pre_derived_group_by_features = set(
            get_pre_derived_group_by_features(jp.groupBy)
        )
        derived_group_by_features = build_derived_columns(
            pre_derived_group_by_features, jp.groupBy.derivations
        )
        for col in derived_group_by_features:
            prefix = jp.prefix + "_" if jp.prefix else ""
            if join.useLongNames is False:
                gb_prefix = jp.groupBy.metaData.name.replace(".", "_")
            else:
                key_str = "_".join(jp.groupBy.keyColumns)
                gb_prefix = prefix + key_str
            internal_features.append(prefix + gb_prefix + "_" + col)
    return internal_features


def get_pre_derived_source_keys(source: Source) -> List[str]:
    if source.events:
        return list(source.events.query.selects.keys())
    elif source.entities:
        return list(source.entities.query.selects.keys())
    elif source.joinSource:
        return list(source.joinSource.query.selects.keys())


# The logic should be consistent with the full name logic defined
# in https://github.com/airbnb/chronon/blob/main/api/src/main/scala/ai/chronon/api/Extensions.scala#L677.
def get_external_part_full_name(external_part: ExternalPart) -> str:
    prefix = external_part.prefix + "_" if external_part.prefix else ""
    name = external_part.source.metadata.name
    sanitized_name = re.sub("[^a-zA-Z0-9_]", "_", name)
    return "ext_" + prefix + sanitized_name


# The external columns name logic should be consistent with the logic defined in fetcher.scala
# https://github.com/airbnb/chronon/blob/main/online/src/main/scala/ai/chronon/online/Fetcher.scala#L371
def get_pre_derived_external_features(join: Join) -> List[str]:
    external_cols = []
    if join.onlineExternalParts:
        for external_part in join.onlineExternalParts:
            original_external_columns = [
                param.name for param in external_part.source.valueSchema.params
            ]
            prefix = get_external_part_full_name(external_part) + "_"
            for col in original_external_columns:
                external_cols.append(prefix + col)
    return external_cols


def get_pre_derived_join_features(join: Join) -> List[str]:
    return get_pre_derived_join_internal_features(
        join
    ) + get_pre_derived_external_features(join)


def build_derived_columns(
    pre_derived_columns: Set[str], derivations: List[Derivation]
) -> List[str]:
    """
    Build the derived columns from pre-derived columns and derivations.
    """
    # if derivations contain star, then all columns are included except the columns which are renamed
    output_columns = pre_derived_columns
    if derivations:
        found = any(derivation.expression == "*" for derivation in derivations)
        if not found:
            output_columns.clear()
        for derivation in derivations:
            if found and is_identifier(derivation.expression):
                output_columns.remove(derivation.expression)
            if derivation.name != "*":
                output_columns.add(derivation.name)
    return list(output_columns)


def get_join_output_columns(join: Join) -> List[str]:
    """
    From the join object, get the final output columns after derivations.
    """
    output_columns = set(
        get_pre_derived_join_features(join) + get_pre_derived_source_keys(join.left)
    )
    if join.derivations:
        return build_derived_columns(output_columns, join.derivations)
    else:
        return list(output_columns)


def _source_has_topic(source: Source) -> bool:
    if source.events:
        return source.events.topic is not None
    elif source.entities:
        return source.entities.mutationTopic is not None
    elif source.joinSource:
        return _source_has_topic(source.joinSource.join.left)
    return False


def _group_by_has_topic(groupBy: GroupBy) -> bool:
    return any(_source_has_topic(source) for source in groupBy.sources)


def _group_by_has_hourly_windows(groupBy: GroupBy) -> bool:
    aggs: List[Aggregation] = groupBy.aggregations

    if not aggs:
        return False

    for agg in aggs:

        if not agg.windows:
            return False

        for window in agg.windows:
            if window.timeUnit == common.TimeUnit.HOURS:
                return True

    return False


def detect_feature_name_collisions(
        group_bys: List[Tuple[GroupBy, str]],
        entity_set_type: str,
        name: str
) -> BaseException | None:
    # Build a map of output_column -> set of group_by name
    output_col_to_gbs = {}
    for gb, prefix in group_bys:
        jp_prefix_str = f"{prefix}_" if prefix else ""
        key_str = "_".join(gb.keyColumns)
        prefix_str = jp_prefix_str + key_str + "_"
        cols = {
            f"{prefix_str}{base_col}"
            for base_col in get_group_by_output_columns(gb, exclude_keys=True)
        }
        gb_name = gb.metaData.name
        for col in cols:
            if col not in output_col_to_gbs:
                output_col_to_gbs[col] = set()
            output_col_to_gbs[col].add(gb_name)

    # Find output columns that are produced by more than one group_by
    collisions = {
        col: gb_names
        for col, gb_names in output_col_to_gbs.items()
        if len(gb_names) > 1
    }

    if not collisions:
        return None  # no collisions

    # Assemble an error message listing the conflicting GroupBys by name
    lines = [
        f"{entity_set_type} for Join: {name} has the following output name collisions:\n"
    ]
    for col, gb_names in collisions.items():
        names_str = ", ".join(sorted(gb_names))
        lines.append(f"  - [{col}] has collisions from: [{names_str}]")

    lines.append(
        "\nConsider assigning distinct `prefix` values to the conflicting parts to avoid collisions."
    )
    return ValueError("\n".join(lines))


class ConfValidator(object):
    """
    applies repo wide validation rules
    """

    def __init__(
        self,
        input_root,
        output_root,
        existing_gbs,
        existing_joins,
        log_level=logging.INFO,
    ):

        self.chronon_root_path = input_root
        self.output_root = output_root

        self.log_level = log_level
        self.logger = get_logger(log_level)

        # we keep the objs in the list not in  a set since thrift does not
        # implement __hash__ for ttypes object.

        self.old_objs = defaultdict(dict)
        self.old_group_bys = existing_gbs
        self.old_joins = existing_joins
        self.old_objs["GroupBy"] = self.old_group_bys
        self.old_objs["Join"] = self.old_joins

    def _get_old_obj(self, obj_class: type, obj_name: str) -> object:
        """
        returns:
           materialized version of the obj given the object's name.
        """
        class_name = obj_class.__name__

        if class_name not in self.old_objs:
            return None
        obj_map = self.old_objs[class_name]

        if obj_name not in obj_map:
            return None
        return obj_map[obj_name]

    def _get_old_joins_with_group_by(self, group_by: GroupBy) -> List[Join]:
        """
        returns:
            materialized joins including the group_by as dicts.
        """
        joins = []
        for join in self.old_joins.values():
            if join.joinParts is not None and group_by.metaData.name in [
                rp.groupBy.metaData.name for rp in join.joinParts
            ]:
                joins.append(join)
        return joins

    def can_skip_materialize(self, obj: object) -> List[str]:
        """
        Check if the object can be skipped to be materialized and return reasons
        if it can be.
        """
        reasons = []
        if isinstance(obj, GroupBy):
            if not _is_batch_upload_needed(obj):
                reasons.append(
                    "GroupBys should not be materialized if batch upload job is not needed"
                )
            # Otherwise group_bys included in online join or are marked explicitly
            # online itself are materialized.
            elif not any(
                join.metaData.online for join in self._get_old_joins_with_group_by(obj)
            ) and not _is_batch_upload_needed(obj):
                reasons.append(
                    "is not marked online/production nor is included in any online join"
                )
        return reasons

    def validate_obj(self, obj: object) -> List[BaseException]:
        """
        Validate Chronon API obj against other entities in the repo.

        returns:
          list of errors.
        """
        if isinstance(obj, GroupBy):
            return self._validate_group_by(obj)
        elif isinstance(obj, Join):
            return self._validate_join(obj)
        return []

    def _has_diff(
        self, obj: object, old_obj: object, skipped_fields=SKIPPED_FIELDS
    ) -> bool:
        new_json = {
            k: v
            for k, v in json.loads(thrift_simple_json(obj)).items()
            if k not in skipped_fields
        }
        old_json = {
            k: v
            for k, v in json.loads(thrift_simple_json(old_obj)).items()
            if k not in skipped_fields
        }
        if isinstance(obj, Join):
            _filter_skipped_fields_from_join(new_json, skipped_fields)
            _filter_skipped_fields_from_join(old_json, skipped_fields)
        return new_json != old_json

    def safe_to_overwrite(self, obj: object) -> bool:
        """When an object is already materialized as online, it is no more safe
        to materialize and overwrite the old conf.
        """
        old_obj = self._get_old_obj(type(obj), obj.metaData.name)
        return (
            not old_obj
            or not self._has_diff(obj, old_obj)
            or not old_obj.metaData.online
        )

    def _validate_derivations(
        self, pre_derived_cols: List[str], derivations: List[Derivation]
    ) -> List[BaseException]:
        """
        Validate join/groupBy's derivation is defined correctly.

        Returns:
          list of validation errors.
        """
        errors = []
        derived_columns = set(pre_derived_cols)

        wild_card_derivation_included = any(
            derivation.expression == "*" for derivation in derivations
        )
        if not wild_card_derivation_included:
            derived_columns.clear()
        for derivation in derivations:
            # if the derivation is a renaming derivation, check whether the expression is in pre-derived schema
            if is_identifier(derivation.expression):
                # for wildcard derivation we want to remove the original column if there is a renaming operation
                # applied on it
                if wild_card_derivation_included:
                    if derivation.expression in derived_columns:
                        derived_columns.remove(derivation.expression)
                if (
                    derivation.expression not in pre_derived_cols
                    and derivation.expression not in ("ds", "ts")
                ):
                    errors.append(
                        ValueError("Incorrect derivation expression {}, expression not found in pre-derived columns {}"
                        .format(
                            derivation.expression, pre_derived_cols
                        ))
                    )
            if derivation.name != "*":
                if derivation.name in derived_columns:
                    errors.append(
                        ValueError("Incorrect derivation name {} due to output column name conflict".format(
                            derivation.name
                        )
                    ))
                else:
                    derived_columns.add(derivation.name)
        return errors

    def _validate_join_part_keys(self, join_part: JoinPart, left_cols: List[str]) -> BaseException:
        keys = []

        key_mapping = join_part.keyMapping if join_part.keyMapping else {}
        for key in join_part.groupBy.keyColumns:
            keys.append(key_mapping.get(key, key))

        missing = [k for k in keys if k not in left_cols]

        err_string = ""
        left_cols_as_str = ", ".join(left_cols)
        group_by_name = join_part.groupBy.metaData.name
        if missing:
            key_mapping_str = f"Key Mapping: {key_mapping}" if key_mapping else ""
            err_string += textwrap.dedent(f"""
                - Join is missing keys {missing} on left side. Required for JoinPart: {group_by_name}. 
                Existing columns on left side: {left_cols_as_str}
                All required Keys: {join_part.groupBy.keyColumns}
                {key_mapping_str}
                Consider renaming a column on the left, or including the key_mapping argument to your join_part.""")

        if key_mapping:
            # Left side of key mapping should include columns on the left
            key_map_keys_missing_from_left = [k for k in key_mapping.keys() if k not in left_cols]
            if key_map_keys_missing_from_left:
                err_string += f"\n- The following keys in your key_mapping: {str(key_map_keys_missing_from_left)} for JoinPart {group_by_name} are not included in the left side of the join: {left_cols_as_str}"

            # Right side of key mapping should only include keys in GroupBy
            keys_missing_from_key_map_values = [v for v in key_mapping.values() if v not in join_part.groupBy.keyColumns]
            if keys_missing_from_key_map_values:
                err_string += f"\n- The following values in your key_mapping: {str(keys_missing_from_key_map_values)}  for JoinPart {group_by_name} do not cover any group by key columns: {join_part.groupBy.keyColumns}"

            if key_map_keys_missing_from_left or keys_missing_from_key_map_values:
                err_string += "\n(Key Mapping should be formatted as column_from_left -> group_by_key)"

        if err_string:
            return ValueError(err_string)


    def _validate_keys(self, join: Join) -> List[BaseException]:
        left = join.left

        left_selects = None
        if left.events:
            left_selects = left.events.query.selects
        elif left.entities:
            left_selects = left.entities.query.selects
        elif left.joinSource:
            left_selects = left.joinSource.query.selects
            # TODO -- if selects are not selected here, get output cols from join

        left_cols = []

        if left_selects:
            left_cols = left_selects.keys()

        errors = []

        if left_cols:
            join_parts = join.joinParts

            # Add label_parts to join_parts to validate if set
            label_parts = join.labelParts
            if label_parts:
                for label_jp in label_parts.labels:
                    join_parts.append(label_jp)

            # Validate join_parts
            for join_part in join_parts:
                join_part_err = self._validate_join_part_keys(join_part, left_cols)
                if join_part_err:
                    errors.append(join_part_err)

        return errors


    def _validate_join(self, join: Join) -> List[BaseException]:
        """
        Validate join's status with materialized versions of group_bys
        included by the join.

        Returns:
          list of validation errors.
        """
        included_group_bys_and_prefixes = [(rp.groupBy, rp.prefix) for rp in join.joinParts]
        # TODO: Remove label parts check in future PR that deprecates label_parts
        included_label_parts_and_prefixes = [(lp.groupBy, lp.prefix) for lp in join.labelParts.labels] if join.labelParts else []
        included_group_bys = [tup[0] for tup in included_group_bys_and_prefixes]

        offline_included_group_bys = [
            gb.metaData.name
            for gb in included_group_bys
            if not gb.metaData or gb.metaData.online is False
        ]
        errors = []
        old_group_bys = [
            group_by
            for group_by in included_group_bys
            if self._get_old_obj(GroupBy, group_by.metaData.name)
        ]
        non_prod_old_group_bys = [
            group_by.metaData.name
            for group_by in old_group_bys
            if group_by.metaData.production is False
        ]
        # Check if the underlying groupBy is valid
        group_by_errors = [
            self._validate_group_by(group_by) for group_by in included_group_bys
        ]
        errors += [
            ValueError(f"join {join.metaData.name}'s underlying {error}")
            for errors in group_by_errors
            for error in errors
        ]
        # Check if the production join is using non production groupBy
        if join.metaData.production and non_prod_old_group_bys:
            errors.append(
                ValueError("join {} is production but includes the following non production group_bys: {}".format(
                    join.metaData.name, ", ".join(non_prod_old_group_bys)
                )
            ))
        # Check if the online join is using the offline groupBy
        if join.metaData.online:
            if offline_included_group_bys:
                errors.append(
                    ValueError("join {} is online but includes the following offline group_bys: {}".format(
                        join.metaData.name, ", ".join(offline_included_group_bys)
                    )
                ))
        # Only validate the join derivation when the underlying groupBy is valid
        group_by_correct = all(not errors for errors in group_by_errors)
        if join.derivations and group_by_correct:
            features = get_pre_derived_join_features(join)
            # For online joins keys are not included in output schema
            if join.metaData.online:
                columns = features
            else:
                keys = get_pre_derived_source_keys(join.left)
                columns = features + keys
            errors.extend(self._validate_derivations(columns, join.derivations))


        errors.extend(self._validate_keys(join))

        # If the join is using "short" names, ensure that there are no collisions
        if join.useLongNames is False:
            right_part_collisions = detect_feature_name_collisions(included_group_bys_and_prefixes, "right parts", join.metaData.name)
            if right_part_collisions:
                errors.append(right_part_collisions)

            label_part_collisions = detect_feature_name_collisions(included_label_parts_and_prefixes, "label parts", join.metaData.name)
            if label_part_collisions:
                errors.append(label_part_collisions)

        return errors


    def _validate_group_by(self, group_by: GroupBy) -> List[BaseException]:
        """
        Validate group_by's status with materialized versions of joins
        including the group_by.

        Return:
          List of validation errors.
        """
        joins = self._get_old_joins_with_group_by(group_by)
        online_joins = [
            join.metaData.name for join in joins if join.metaData.online is True
        ]
        prod_joins = [
            join.metaData.name for join in joins if join.metaData.production is True
        ]
        errors = []

        non_temporal = (
            group_by.accuracy is None or group_by.accuracy == Accuracy.SNAPSHOT
        )

        no_topic = not _group_by_has_topic(group_by)
        has_hourly_windows = _group_by_has_hourly_windows(group_by)

        # batch features cannot contain hourly windows
        if (no_topic and non_temporal) and has_hourly_windows:
            errors.append(
                ValueError(f"group_by {group_by.metaData.name} is defined to be daily refreshed but contains "
                           f"hourly windows. "
                           )
            )

        # group by that are marked explicitly offline should not be present in
        # materialized online joins.
        if group_by.metaData.online is False and online_joins:
            errors.append(
                ValueError("group_by {} is explicitly marked offline but included in "
                "the following online joins: {}".format(
                    group_by.metaData.name, ", ".join(online_joins)
                ))
            )
        # group by that are marked explicitly non-production should not be
        # present in materialized production joins.
        if prod_joins:
            if group_by.metaData.production is False:
                errors.append(
                    ValueError(
                        "group_by {} is explicitly marked as non-production but included in the following production "
                    "joins: {}".format(group_by.metaData.name, ", ".join(prod_joins))
                ))
            # if the group by is included in any of materialized production join,
            # set it to production in the materialized output.
            else:
                group_by.metaData.production = True

        # validate the derivations are defined correctly
        if group_by.derivations:
            # For online group_by keys are not included in output schema
            if group_by.metaData.online:
                columns = get_pre_derived_group_by_features(group_by)
            else:
                columns = get_pre_derived_group_by_columns(group_by)
            errors.extend(self._validate_derivations(columns, group_by.derivations))

        for source in group_by.sources:
            src: Source = source
            if (
                src.events
                and src.events.isCumulative
                and (src.events.query.timeColumn is None)
            ):
                errors.append(
                    ValueError("Please set query.timeColumn for Cumulative Events Table: {}".format(
                        src.events.table
                    ))
                )
            elif (
                src.joinSource
            ):
                join_obj = src.joinSource.join
                if join_obj.metaData.name is None or join_obj.metaData.team is None:
                    errors.append(
                        ValueError(f"Join must be defined with team and name: {join_obj}")
                    )
        return errors
