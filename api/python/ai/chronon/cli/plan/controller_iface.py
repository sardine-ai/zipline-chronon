from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from ai.chronon.orchestration.ttypes import (
    DiffResponse,
    NodeInfo,
)


class ControllerIface(ABC):
    """
    Class used to make the rest of the planner code agnostic to the underlying orchestrator.
    Mainly used to mock out the orchestrator for testing.
    """

    @abstractmethod
    def fetch_missing_confs(self, node_to_hash: Dict[str, str]) -> DiffResponse:
        # req = DiffRequest(namesToHashes=node_to_hash)
        # TODO -- call API
        pass

    @abstractmethod
    def upload_branch_mappsing(self, node_info: List[NodeInfo], branch: str):
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
