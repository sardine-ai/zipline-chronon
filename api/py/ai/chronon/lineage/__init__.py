from dataclasses import dataclass
from enum import Enum
from typing import Dict, List
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
    name: str
    transform_clause: Clause
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


def get_lineage_from_query(obj: chronon.Query) -> RowTransform:
    """
    used in source & bootstraps
    """
    pass


def get_lineage_from_staging_query(obj: chronon.StagingQuery) -> RowTransform:
    pass


def get_lineage_from_derivations(derivations: List[chronon.Derivation]) -> RowTransform:
    """
    used in derivations and selects of the query
    """
    pass


def get_lineage_from_group_by(obj: chronon.GroupBy) -> RowTransform:
    """
    input_col --(select_expr)--> selected_col --(agg)--> agg_col --(derivation)--> output_col

    one_pipeline:
    select_transform = ColumnTransform(input_col, selected_col, select_expr, [])
    agg_transform = ColumnTransform(selected_col, agg_col, agg_expr, [])
    derivation_transform = ColumnTransform(agg_col, output_col, derivation_expr, [])

    """
    pass


#  intermediate physical node in join backfills
def get_lineage_for_bootstrap(
    left: chronon.Source, bootstraps: List[chronon.BootstrapPart]
) -> RowTransform:
    pass


#  intermediate physical node in join backfills
def get_lineage_for_join_part(
    left: chronon.Source, join_part: chronon.JoinPart
) -> RowTransform:
    pass


#  intermediate physical node in join backfills
def get_lineage_for_join_without_derivations(join: chronon.Join) -> RowTransform:
    pass


def get_lineage_for_join(join: chronon.Join) -> RowTransform:
    pass


def get_lineage_for_label_parts(join: chronon.Join) -> RowTransform:
    pass
