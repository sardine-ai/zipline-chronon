"""
TUI (Text User Interface) module for interactive DAG visualization.

This module provides a clean, modular interface for creating interactive
DAG (Directed Acyclic Graph) visualizers using the Textual framework.
"""

from .models import StepStatus
from .widgets import ProgressGrid
from .app import InteractiveDAGApp
from .demo import run_example, create_example_data

__all__ = [
    "StepStatus",
    "ProgressGrid", 
    "InteractiveDAGApp",
    "run_example",
    "create_example_data"
]