from dataclasses import dataclass
from typing import Any, Dict, List, Set
from rich.live import Live
from rich.text import Text
from rich.console import Console
from collections import defaultdict


@dataclass
class CompiledObj:
    name: str
    obj: Any
    file: str
    error: Exception


class CompileStatus:
    def __init__(self):
        self.console = Console()
        self.stats: Dict[str, int] = defaultdict(
            int
        )  # Track successful compiles by type
        self.current_files: Dict[str, str] = {}  # Track current file for each type
        self.error_files: Set[str] = set()  # Track unique files with errors
        self.errors: List[tuple[str, str, Exception]] = []  # Track (type, file, error)
        self.live = Live("", console=self.console, refresh_per_second=4)

    def add_object(self, compiled: CompiledObj) -> None:
        """Process a new compiled object and update the display."""
        obj_type = compiled.obj.__class__.__name__ if compiled.obj else "Unknown"

        if compiled.error:
            self.error_files.add(compiled.file)
            self.errors.append((obj_type, compiled.file, compiled.error))
        else:
            self.stats[obj_type] += 1
            self.current_files[obj_type] = compiled.file

        self._update_display()

    def _generate_display(self) -> Text:
        """Generate the full status display."""
        text = Text()

        # Add statistics for each object type
        for obj_type, count in sorted(self.stats.items()):
            text.append(f"{obj_type}: ", style="bold cyan")
            text.append(f"{count} objects", style="green")
            text.append(f" ({len(self.error_files)} files with errors)", style="yellow")

            # Add current file if available
            if obj_type in self.current_files:
                text.append(" - processing: ", style="dim")
                text.append(self.current_files[obj_type], style="blue")

            text.append("\n")

        if not self.stats:
            text.append("No objects compiled yet\n", style="yellow")

        # Add error section if there are any
        if self.errors:
            text.append("\nErrors:\n", style="bold red")
            for obj_type, file, error in self.errors[-10:]:  # Show last 10 errors
                text.append(f"[{obj_type}] ", style="cyan")
                text.append(f"{file}: ", style="yellow")
                text.append(f"{str(error)}\n", style="red")

            if len(self.errors) > 10:
                text.append(
                    f"... and {len(self.errors) - 10} more errors\n", style="dim"
                )

        return text

    def _update_display(self) -> None:
        """Update the live display."""
        self.live.update(self._generate_display())

    def __enter__(self):
        """Start the live display."""
        self.live.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up the live display."""
        self.live.__exit__(exc_type, exc_val, exc_tb)


# Example usage:
if __name__ == "__main__":
    import time

    # Example objects for testing
    class TestObj:
        pass

    class AnotherObj:
        pass

    with CompileStatus() as status:
        # Simulate some compilations
        objs = [
            CompiledObj("test1", TestObj(), "file1.py", None),
            CompiledObj("test2", TestObj(), "file2.py", None),
            CompiledObj("test3", None, "file3.py", ValueError("Invalid syntax")),
            CompiledObj("test4", AnotherObj(), "file4.py", None),
            CompiledObj("test5", None, "file5.py", ImportError("Module not found")),
        ]

        for obj in objs:
            status.add_object(obj)
            time.sleep(1)  # Simulate processing time
