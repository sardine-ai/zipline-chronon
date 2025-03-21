from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from ai.chronon.cli.plan.physical_graph import PhysicalGraph


class ControllerIface(ABC):
    """
    Class used to make the rest of the planner code agnostic to the underlying orchestrator.
    Mainly used to mock out the orchestrator for testing.
    """

    @abstractmethod
    def fetch_missing_confs(self, node_to_hash: Dict[str, str]) -> List[str]:
        pass

    @abstractmethod
    def upload_conf(self, name: str, hash: str, content: str) -> None:
        pass

    @abstractmethod
    def create_workflow(
        self, physical_graph: PhysicalGraph, start_date: str, end_date: str
    ) -> str:
        """
        Submit a physical graph to the orchestrator and return workflow id
        """
        pass

    @abstractmethod
    def get_workflow_status(self, workflow_id: str) -> str:
        """
        Get the status of a workflow
        """
        pass

    @abstractmethod
    def get_active_workflows(
        self, branch: Optional[str] = None, user: Optional[str] = None
    ) -> List[str]:
        """
        List all active workflows
        """
        pass
