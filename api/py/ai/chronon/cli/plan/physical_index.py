from dataclasses import dataclass
from typing import Dict, List

from ai.chronon.api.common.ttypes import ConfigType
from ai.chronon.cli.compile.compiler import CompileResult
from ai.chronon.cli.plan.controller_iface import ControllerIface
from ai.chronon.cli.plan.physical_graph import PhysicalGraph
from ai.chronon.lineage.ttypes import Column, ColumnLineage
from ai.chronon.orchestration.ttypes import PhysicalNode


@dataclass
class PhysicalIndex:
    table_to_physical: Dict[str, PhysicalNode]

    # TODO incorporate stage column lineage
    column_lineage: Dict[Column, ColumnLineage]
    controller: ControllerIface

    def __init__(
        self,
        physical_nodes: List[PhysicalNode],
        controller: ControllerIface,
        branch: str,
    ):
        self.controller = controller
        self.table_to_physical = {}
        self.column_lineage = {}
        self.branch = branch
        self.populate_index(physical_nodes)

    # TODO: populate index
    def populate_index(self, physical_nodes: List[PhysicalNode]):
        raise NotImplementedError("Method not yet implemented")

    @classmethod
    def from_compiled_obj(
        cls, compiled_obj: Dict[ConfigType, CompileResult]
    ) -> "PhysicalIndex":
        raise NotImplementedError("Method not yet implemented")

    def get_backfill_physical_graph(
        self, conf_name: str, start_date: str, end_date: str
    ) -> PhysicalGraph:
        raise NotImplementedError("Method not yet implemented")

    def get_deploy_physical_graph(self, conf_name: str, date: str) -> PhysicalGraph:
        raise NotImplementedError("Method not yet implemented")

    def submit_physical_graph(self, physical_graph: PhysicalGraph) -> str:

        node_to_physical: Dict[str, PhysicalNode] = physical_graph.flatten()

        node_to_hash = {name: node.conf_hash for name, node in node_to_physical.items()}

        missing_conf_names = self.controller.fetch_missing_confs(node_to_hash)
        missing_physical_nodes = [
            node_to_physical[conf_name] for conf_name in missing_conf_names
        ]

        # upload missing confs
        for physical_node in missing_physical_nodes:
            hash = physical_node.conf_hash
            json = physical_node.conf.tjson
            name = physical_node.name
            self.controller.upload_conf(name, hash, json)
