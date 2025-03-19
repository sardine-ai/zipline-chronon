from dataclasses import dataclass
from typing import Dict, List

from ai.chronon.cli.plan.physical_index import PhysicalNode


@dataclass
class PhysicalGraph:
    node: PhysicalNode
    dependencies: List["PhysicalGraph"]
    start_date: str
    end_date: str

    def flatten(self) -> Dict[str, PhysicalNode]:
        # recursively find hashes of all nodes in the physical graph

        result = {self.node.name: self.node}

        for sub_graph in self.dependencies:
            sub_hashes = sub_graph.flatten()
            result.update(sub_hashes)

        return result
