"""
Data models for the TUI module.
"""

from dataclasses import dataclass
from datetime import date


@dataclass
class StepStatus:
    """Status information for a step within a node."""
    start_date: date
    end_date: date 
    status: str  # 'empty', 'finished', 'running', 'failed'
    details: str = ""  # Additional details about the step