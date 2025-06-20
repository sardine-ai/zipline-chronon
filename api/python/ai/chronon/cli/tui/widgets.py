"""
Custom widgets for the TUI module.
"""

from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from datetime import date, timedelta
from textual.widgets import Static
from textual import events
from rich.text import Text
from rich.console import RenderableType

from .models import StepStatus


class ProgressGrid(Static):
    """A navigable progress grid widget."""
    
    def __init__(self, dependencies: Dict[str, str], root: str, 
                 date_range: tuple[date, date],
                 node_statuses: Optional[Dict[str, List[StepStatus]]] = None):
        super().__init__()
        self.dependencies = dependencies
        self.root = root
        self.start_date, self.end_date = date_range
        self.node_statuses = node_statuses or {}
        self.children_map = self._build_children_map()
        self.date_list = self._generate_date_list()
        self.node_list = self._build_node_list()
        self.steps = self._build_steps_list()  # List of (node, step_index) tuples
        self.current_step_index = 0
        self._find_first_valid_step()
        
        # Horizontal scrolling
        self.h_scroll_offset = 0
        self.viewport_width = 60  # Initial value, will be updated in render()
        
    def _get_visible_dates(self) -> List[date]:
        """Get the currently visible dates based on scroll offset."""
        start_idx = self.h_scroll_offset
        end_idx = min(start_idx + self.viewport_width, len(self.date_list))
        return self.date_list[start_idx:end_idx]
    
    def _scroll_horizontal(self, direction: str):
        """Scroll horizontally through the timeline."""
        if direction == "right":
            max_offset = max(0, len(self.date_list) - self.viewport_width)
            self.h_scroll_offset = min(self.h_scroll_offset + 7, max_offset)  # Scroll by week
        elif direction == "left":
            self.h_scroll_offset = max(0, self.h_scroll_offset - 7)  # Scroll by week
            
    def _auto_scroll_to_current_step(self):
        """Auto-scroll to show the current step if it's outside the viewport."""
        current_step = self._get_current_step()
        if not current_step:
            return
            
        # Find the date range of the current step
        step_start_idx = None
        step_end_idx = None
        for i, date_item in enumerate(self.date_list):
            if current_step.start_date <= date_item <= current_step.end_date:
                if step_start_idx is None:
                    step_start_idx = i
                step_end_idx = i
        
        if step_start_idx is None:
            return
            
        # Check if step is visible in current viewport
        viewport_start = self.h_scroll_offset
        viewport_end = self.h_scroll_offset + self.viewport_width
        
        # If step is not visible, scroll to show it
        if step_start_idx < viewport_start:
            self.h_scroll_offset = max(0, step_start_idx - 5)  # Add some padding
        elif step_end_idx >= viewport_end:
            self.h_scroll_offset = min(step_end_idx - self.viewport_width + 5, 
                                     len(self.date_list) - self.viewport_width)
        
    def _build_children_map(self) -> Dict[str, List[str]]:
        """Build a map from parent to list of children."""
        children_map = defaultdict(list)
        for child, parent in self.dependencies.items():
            children_map[parent].append(child)
        return dict(children_map)
    
    def _generate_date_list(self) -> List[date]:
        """Generate list of dates in the range."""
        dates = []
        current = self.start_date
        while current <= self.end_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates
    
    def _build_node_list(self) -> List[str]:
        """Build ordered list of nodes for the tree."""
        result = []
        
        def traverse(node: str):
            result.append(node)
            for child in sorted(self.children_map.get(node, [])):
                traverse(child)
        
        traverse(self.root)
        return result
    
    def _build_steps_list(self) -> List[Tuple[str, int]]:
        """Build a list of all steps (node, step_index) in order."""
        return [(node, step_index) 
                for node in self.node_list 
                if node in self.node_statuses
                for step_index in range(len(self.node_statuses[node]))]
    
    def _find_first_valid_step(self):
        """Find the first valid step."""
        self.current_step_index = 0 if self.steps else -1
    
    def _get_status_for_date(self, node: str, target_date: date) -> Optional[StepStatus]:
        """Get status object for a node on a specific date."""
        return next((status for status in self.node_statuses.get(node, [])
                    if status.start_date <= target_date <= status.end_date), None)
    
    def _get_current_step(self) -> Optional[StepStatus]:
        """Get the currently selected step."""
        if not self.steps or self.current_step_index >= len(self.steps):
            return None
        
        node, step_index = self.steps[self.current_step_index]
        return self.node_statuses.get(node, [])[step_index] if step_index < len(self.node_statuses.get(node, [])) else None
    
    def _get_status_color(self, status: str) -> str:
        """Get color for status."""
        return {'finished': 'dark_green', 'running': 'bright_green', 'failed': 'dark_red'}.get(status, 'white')
    
    def _find_node_step(self, target_node: str, target_step_idx: int) -> Optional[int]:
        """Find the index of a specific step in the steps list."""
        for i, (node, step_idx) in enumerate(self.steps):
            if node == target_node and step_idx == target_step_idx:
                return i
        return None

    def _move_vertically(self, direction: str):
        """Move to next/previous node, maintaining step position."""
        current_node, current_step_idx = self.steps[self.current_step_index]
        current_node_index = self.node_list.index(current_node)
        
        node_range = (range(current_node_index + 1, len(self.node_list)) if direction == "down" 
                     else range(current_node_index - 1, -1, -1))
        
        for node_index in node_range:
            target_node = self.node_list[node_index]
            if target_node in self.node_statuses:
                target_step_idx = min(current_step_idx, len(self.node_statuses[target_node]) - 1)
                step_index = self._find_node_step(target_node, target_step_idx)
                if step_index is not None:
                    self.current_step_index = step_index
                    return

    def _move_to_next_step(self, direction: str):
        """Move to next/previous step with smart navigation."""
        if direction in ["up", "down"]:
            self._move_vertically(direction)
                            
        elif direction == "right":
            # Move to next step chronologically
            if self.current_step_index < len(self.steps) - 1:
                self.current_step_index += 1
                    
        elif direction == "left":
            # Move to previous step chronologically
            if self.current_step_index > 0:
                self.current_step_index -= 1
    
    def _get_step_boundaries(self, node: str, step_obj: StepStatus) -> tuple[int, int]:
        """Get the start and end column indices for a step in the visible viewport."""
        visible_dates = self._get_visible_dates()
        matching_indices = [i for i, date_item in enumerate(visible_dates)
                           if step_obj.start_date <= date_item <= step_obj.end_date]
        return (matching_indices[0], matching_indices[-1]) if matching_indices else (None, None)
    
    def _get_current_highlight_info(self, node: str) -> tuple[Optional[int], Optional[int]]:
        """Get highlighting information for the current node."""
        current_step = self._get_current_step()
        if not current_step or not self.steps:
            return None, None
            
        current_node, _ = self.steps[self.current_step_index]
        if node != current_node:
            return None, None
            
        return self._get_step_boundaries(node, current_step)

    def _create_progress_line(self, node: str) -> Text:
        """Create a progress line for a node with background color highlighting."""
        progress = Text()
        highlight_start, highlight_end = self._get_current_highlight_info(node)
        visible_dates = self._get_visible_dates()
        
        # Add dummy space at start of all progress lines for consistent alignment
        progress.append(" ")
        
        for i, date_item in enumerate(visible_dates):
            status_obj = self._get_status_for_date(node, date_item)
            
            # Check if this position is highlighted
            is_highlighted = highlight_start is not None and highlight_start <= i <= highlight_end
            
            if status_obj:
                color = self._get_status_color(status_obj.status)
                if is_highlighted:
                    # Add gray background for highlighted status
                    progress.append("■", style=f"{color} on bright_black")
                else:
                    progress.append("■", style=color)  # Normal filled box
            else:
                if is_highlighted:
                    # Add gray background for highlighted empty cells
                    progress.append("◦", style="dim on bright_black")
                else:
                    progress.append("◦", style="dim")  # Empty/no status
            
            # Add connectors
            if i < len(visible_dates) - 1:
                next_date = visible_dates[i + 1]
                next_status_obj = self._get_status_for_date(node, next_date)
                
                # Check if connector should be highlighted (between current and next position)
                is_connector_highlighted = (highlight_start is not None and 
                                          highlight_start <= i and i + 1 <= highlight_end)
                
                if (status_obj and next_status_obj and 
                    status_obj.status == next_status_obj.status and
                    status_obj.start_date <= next_date <= status_obj.end_date):
                    connector_color = self._get_status_color(status_obj.status)
                    
                    if is_connector_highlighted:
                        progress.append("─", style=f"{connector_color} on bright_black")
                    else:
                        progress.append("─", style=connector_color)
                elif status_obj and next_status_obj:
                    # Both sides have status but different - use dot
                    if is_connector_highlighted:
                        progress.append("·", style="dim on bright_black")
                    else:
                        progress.append("·", style="dim")
                else:
                    # At least one side is empty - use space
                    if is_connector_highlighted:
                        progress.append(" ", style="on bright_black")
                    else:
                        progress.append(" ")
        
        # Add dummy space at end of all progress lines for consistent alignment
        progress.append(" ")
        
        return progress
    
    
    def _build_tree_with_progress(self) -> List[Tuple[str, Text]]:
        """Build tree structure with progress bars using proper tree formatting."""
        def traverse(parent: str, prefix: str = "") -> List[Tuple[str, Text]]:
            tree_part = prefix + parent
            progress_bar = self._create_progress_line(parent)
            result = [(tree_part, progress_bar)]
            
            children = sorted(self.children_map.get(parent, []))
            for i, child in enumerate(children):
                is_last_child = (i == len(children) - 1)
                
                # Child connector and extension for deeper levels
                if is_last_child:
                    child_connector = "└── "
                    extension = "    "  # Empty space under last child
                else:
                    child_connector = "├── "
                    extension = "│   "  # Vertical line continues
                
                # Recursively build child tree with proper prefix
                child_results = traverse(child, prefix + extension)
                
                # Add child with its connector
                child_tree_part = prefix + child_connector + child_results[0][0][len(prefix + extension):]
                result.append((child_tree_part, child_results[0][1]))
                
                # Add grandchildren with proper indentation
                for grandchild_line, grandchild_progress in child_results[1:]:
                    result.append((prefix + extension + grandchild_line[len(prefix + extension):], grandchild_progress))
            
            return result
        
        return traverse(self.root)
    
    def _create_date_row(self) -> Text:
        """Create a date row showing week starts (Mondays) in MM/DD format."""
        date_row = Text()
        visible_dates = self._get_visible_dates()
        
        # Add dummy space at start to align with progress lines
        date_row.append(" ")
        
        # Build the date row - only add full week spacing if there are enough days left
        i = 0
        while i < len(visible_dates):
            date_item = visible_dates[i]
            
            # Check if this date is a Monday (weekday() returns 0 for Monday)
            if date_item.weekday() == 0:
                # Check if we have a full week (7 days) remaining in visible dates
                days_remaining = len(visible_dates) - i
                if days_remaining >= 7:
                    # Show MM/DD format (5 characters with padding) + 9 spaces for full week
                    date_str = date_item.strftime("%m/%d").ljust(5)  # Pad to 5 chars
                    date_row.append(date_str, style="dim")
                    date_row.append("         ")  # 9 spaces for rest of week
                    i += 7  # Skip to next Monday (7 days = 1 week)
                else:
                    # Partial week - just show date and continue day by day
                    date_str = date_item.strftime("%m/%d")
                    date_row.append(date_str, style="dim")
                    i += 1
            else:
                # Add 2 spaces to match cell + connector spacing
                date_row.append("  ")
                i += 1
        
        # Add dummy space at end to align with progress lines  
        date_row.append(" ")
        
        return date_row


    def render(self) -> RenderableType:
        """Render the progress grid with bracket highlighting."""
        from rich.table import Table
        from rich.console import Console
        
        # Update viewport width based on available console width
        console = Console()
        terminal_width = console.size.width
        
        # Estimate space for first column (tree + longest node name)
        max_node_length = max(len(node) for node in self.node_list) if self.node_list else 20
        first_col_estimate = max_node_length + 15  # Tree symbols + padding
        
        # Available space for progress column (each day takes ~2 chars)
        available_width = terminal_width - first_col_estimate - 10  # Border padding
        new_viewport_width = max(30, available_width // 2)  # Minimum 30 days
        
        # Update viewport width if it changed significantly
        if abs(new_viewport_width - self.viewport_width) > 5:
            self.viewport_width = min(new_viewport_width, len(self.date_list))
        
        tree_data = self._build_tree_with_progress()
        date_row = self._create_date_row()
        
        # Create date range for first row
        visible_dates = self._get_visible_dates()
        start_date = visible_dates[0].strftime("%Y/%m/%d")
        end_date = visible_dates[-1].strftime("%Y/%m/%d")
        date_range_text = f"{start_date} - {end_date}"
        
        from rich.box import SIMPLE
        from rich.text import Text
        
        table = Table(show_header=True, box=SIMPLE, border_style="dim", expand=True, header_style="", padding=(0, 1))
        
        # Create styled headers
        date_range_styled = Text(date_range_text, style="dim")
        
        table.add_column(date_range_styled, no_wrap=True, width=None)
        table.add_column(date_row, ratio=1)
        
        for tree_part, progress_bar in tree_data:
            table.add_row(tree_part, progress_bar)
        
        # Wrap the table in a Panel titled "Workflow Progress"
        from rich.panel import Panel
        
        workflow_panel = Panel(table, title="Workflow Progress", border_style="dim")
        
        return workflow_panel
    
    def get_current_details(self) -> str:
        """Get details for the currently selected step."""
        current_step = self._get_current_step()
        if not current_step or not self.steps:
            return "No step selected"
        
        node, step_index = self.steps[self.current_step_index]
        
        # Include scroll information in details
        visible_dates = self._get_visible_dates()
        start_date = visible_dates[0].strftime("%m/%d")
        end_date = visible_dates[-1].strftime("%m/%d")
        total_days = len(self.date_list)
        visible_days = len(visible_dates)
        
        scroll_info = f"Showing {start_date} - {end_date} ({self.h_scroll_offset + 1}-{self.h_scroll_offset + visible_days} of {total_days})"
        
        return f"Node: {node}\nStep: {step_index + 1}/{len(self.node_statuses[node])}\nDates: {current_step.start_date} to {current_step.end_date}\nStatus: {current_step.status}\nDetails: {current_step.details}\n\nTimeline: {scroll_info}"
    
    async def on_key(self, event: events.Key) -> None:
        """Handle key presses for navigation."""
        if event.key in ["up", "down", "left", "right"]:
            self._move_to_next_step(event.key)
            self._auto_scroll_to_current_step()
            self.refresh()