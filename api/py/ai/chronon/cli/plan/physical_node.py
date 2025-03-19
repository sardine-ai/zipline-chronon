from dataclasses import dataclass
from typing import List

from ai.chronon.api.common.ttypes import TableDependency
from ai.chronon.api.ttypes import GroupBy, Join, Model, StagingQuery
from ai.chronon.cli.compile.compile_context import CompiledObj
from ai.chronon.orchestration.ttypes import PhysicalNode


def from_group_by(cls, group_by: GroupBy) -> List[PhysicalNode]:
    raise NotImplementedError("Method not yet implemented")


def from_join(cls, join: Join) -> List[PhysicalNode]:
    raise NotImplementedError("Method not yet implemented")


def from_staging_query(cls, staging_query: StagingQuery) -> List[PhysicalNode]:
    raise NotImplementedError("Method not yet implemented")


def from_model(cls, model: Model) -> List[PhysicalNode]:
    raise NotImplementedError("Method not yet implemented")
