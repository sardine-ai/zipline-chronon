"""
Data models for the TUI module.
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Tuple


@dataclass(frozen=True)
class StepDependency:
    """A dependency reference to another step."""
    node_name: str
    start_date: date
    end_date: date


@dataclass(frozen=True)
class StepStatus:
    """Status information for a step within a node."""
    start_date: date
    end_date: date 
    status: str  # 'empty', 'finished', 'running', 'failed'
    details: str = ""  # Additional details about the step
    step_dependencies: Tuple[StepDependency, ...] = field(default_factory=tuple)  # Tuple of step dependencies