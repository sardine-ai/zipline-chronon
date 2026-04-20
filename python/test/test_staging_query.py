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

import pytest

import gen_thrift.common.ttypes as common
from ai.chronon.staging_query import TableDependency


def test_offset_only_symmetric():
    td = TableDependency(table="ns.upstream", partition_column="ds", offset=0).to_thrift()
    assert td.startOffset == common.Window(length=0, timeUnit=common.TimeUnit.DAYS)
    assert td.endOffset == common.Window(length=0, timeUnit=common.TimeUnit.DAYS)
    assert td.startCutOff is None
    assert td.endCutOff is None


def test_offset_seven_days():
    td = TableDependency(table="ns.upstream", partition_column="ds", offset=7).to_thrift()
    assert td.startOffset == common.Window(length=7, timeUnit=common.TimeUnit.DAYS)
    assert td.endOffset == common.Window(length=7, timeUnit=common.TimeUnit.DAYS)


def test_start_cutoff_only_defaults_end_offset_to_zero():
    # startOffset is None so the planner resolves start = max(null, cutoff) = cutoff.
    # endOffset must be non-null (0 days) so requiredEnd / expandRange see a real date.
    td = TableDependency(
        table="ns.upstream", partition_column="ds", start_cutoff="2024-01-01"
    ).to_thrift()
    assert td.startOffset is None
    assert td.endOffset == common.Window(length=0, timeUnit=common.TimeUnit.DAYS)
    assert td.startCutOff == "2024-01-01"
    assert td.endCutOff is None


def test_offset_and_start_cutoff_both_set():
    td = TableDependency(
        table="ns.upstream",
        partition_column="ds",
        offset=7,
        start_cutoff="2024-01-01",
    ).to_thrift()
    assert td.startOffset == common.Window(length=7, timeUnit=common.TimeUnit.DAYS)
    assert td.endOffset == common.Window(length=7, timeUnit=common.TimeUnit.DAYS)
    assert td.startCutOff == "2024-01-01"


def test_end_cutoff_passthrough():
    td = TableDependency(
        table="ns.upstream",
        partition_column="ds",
        offset=0,
        end_cutoff="2024-12-31",
    ).to_thrift()
    assert td.endCutOff == "2024-12-31"


def test_missing_both_offset_and_cutoff_raises():
    with pytest.raises(ValueError, match="offset or start_cutoff"):
        TableDependency(table="ns.upstream", partition_column="ds").to_thrift()


def test_no_partition_column_is_no_op():
    # Historic behavior: deps without partition_column skip the offset requirement.
    td = TableDependency(table="ns.upstream").to_thrift()
    assert td.tableInfo.partitionColumn is None
