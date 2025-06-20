"""
Main TUI application for the interactive DAG visualizer.
"""

from typing import Dict, List, Optional
from datetime import date
from textual.app import App, ComposeResult
from textual.containers import Vertical
from textual.widgets import Static, Footer
from textual import events

from .models import StepStatus
from .widgets import ProgressGrid


class InteractiveDAGApp(App):
    """Interactive DAG visualizer application."""
    
    BINDINGS = [
        ("q", "quit", "Quit"),
        ("up,down,left,right", "navigate", "Navigate"),
    ]
    
    def __init__(self, dependencies: Dict[str, str], root: str, 
                 date_range: tuple[date, date],
                 node_statuses: Optional[Dict[str, List[StepStatus]]] = None):
        super().__init__()
        self.dependencies = dependencies
        self.root = root
        self.date_range = date_range
        self.node_statuses = node_statuses or {}
        
    def compose(self) -> ComposeResult:
        """Create the app layout."""
        from rich.panel import Panel
        
        self.progress_grid = ProgressGrid(
            self.dependencies, self.root, self.date_range, self.node_statuses
        )
        
        # Create details content and wrap in Panel
        details_content = "Use arrow keys to navigate"
        details_panel = Panel(details_content, title="Step Status", border_style="dim")
        self.details_panel = Static(details_panel, id="details")
        
        yield Vertical(
            self.progress_grid,
            self.details_panel,
        )
        yield Footer()
    
    async def on_key(self, event: events.Key) -> None:
        """Handle global key events."""
        if event.key in ["up", "down", "left", "right"]:
            await self.progress_grid.on_key(event)
            
            # Update details panel with Panel wrapper
            from rich.panel import Panel
            details_content = self.progress_grid.get_current_details()
            details_panel = Panel(details_content, title="Step Status", border_style="dim")
            self.details_panel.update(details_panel)