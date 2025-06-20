#!/usr/bin/env python3
"""
Interactive DAG Tree Visualizer using Textual

This module provides backward compatibility with the original implementation
while using the new modular TUI structure.
"""

from ai.chronon.cli.tui import StepStatus, InteractiveDAGApp, run_example as example_interactive


if __name__ == "__main__":
    example_interactive()