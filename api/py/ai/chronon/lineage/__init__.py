from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Union
import ai.chronon.api.ttypes as chronon


@dataclass
class Clause:
    expr: str
    transforms: List["ColumnTransform"]


@dataclass
class StageInfo:
    name: str
    where_clauses: List[Clause]
    join_clauses: List[Clause]
    key_clauses: List[Clause]


@dataclass
class ColumnTransform:
    output_column: str
    transform_clause: Clause

    # common clauses that apply to all transforms in a stage
    stage_info: StageInfo


def require(cond, msg):
    if not cond:
        raise ValueError(msg)


@dataclass
class RowTransform:
    """
    Every physical node has a row transform.
    But not every row transform has a physical node
    """

    # output column -> column_pipeline
    column_transforms: Dict[str, ColumnTransform]

    def hash(self) -> str:
        pass

    def combine_child(self, child: "RowTransform") -> "RowTransform":
        # TODO:
        pass


@dataclass
class ConfType(Enum):
    JOIN = "join"
    GROUP_BY = "group_by"
    STAGING_QUERY = "staging_query"


@dataclass
class ConfNode:
    conf_name: str
    conf_json: str
    python_file_path: str
    conf_type: ConfType
    conf_obj: Union[chronon.GroupBy, chronon.Join, chronon.StagingQuery]


@dataclass
class PhysicalNodeInfo:
    name: str
    row_transform: RowTransform
    output_table: str
    input_tables: List[str]

    conf_node: ConfNode

    @classmethod
    def from_group_by(cls, group_by: chronon.GroupBy) -> List["PhysicalNodeInfo"]:
        """
        if online = true - generate streaming, upload nodes
        if backfill_start_date is set - generate group_by backfill nodes
        """
        pass

    @classmethod
    def from_join(cls, join: chronon.Join) -> List["PhysicalNodeInfo"]:
        """
        for backfill we generate
            left_table_node, join_part_nodes, join_node, derivation_node, label_join_node
        """
        pass

    @classmethod
    def from_staging_query(
        cls, staging_query: chronon.StagingQuery
    ) -> List["PhysicalNodeInfo"]:
        pass


@dataclass
class PhysicalNodeLineage:
    name: str
    global_row_transform: RowTransform
    output_table: str
    input_nodes_names: List[str]

    row_transform: RowTransform
    conf_node_names: List[str]


@dataclass
class BranchIndex:
    branch_name: str
    conf_dict: Dict[str, ConfNode]
    physical_lineage_dict: Dict[str, PhysicalNodeLineage]

    def publish(self):
        pass

    @classmethod
    def from_all_physical_nodes(
        nodes: List[PhysicalNodeInfo],
    ) -> "BranchIndex":
        """
        1. build dict output_table -> node_info
        2. recurse up the dict to produce full row_transform and convert input_tables to input_nodes
        """
        pass
