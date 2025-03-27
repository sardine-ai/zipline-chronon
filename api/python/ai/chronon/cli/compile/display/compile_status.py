from collections import OrderedDict
from typing import Dict

from rich.live import Live
from rich.text import Text

from ai.chronon.cli.compile.display.class_tracker import ClassTracker
from ai.chronon.cli.compile.display.compiled_obj import CompiledObj


class CompileStatus:
    """
    Uses rich ui - to consolidate and sink the overview of the compile process to the bottom.
    """

    def __init__(self):
        self.cls_to_tracker: Dict[str, ClassTracker] = OrderedDict()
        self.live = Live(refresh_per_second=50)
        self.live.start()

    def add_object_update_display(
        self, compiled: CompiledObj, obj_type: str = None
    ) -> None:

        if compiled.obj_type is not None and obj_type is not None:
            assert (
                compiled.obj_type == obj_type
            ), f"obj_type mismatch: {compiled.obj_type} != {obj_type}"

        if obj_type not in self.cls_to_tracker:
            self.cls_to_tracker[obj_type] = ClassTracker()

        self.cls_to_tracker[obj_type].add(compiled)

        self._update_display()

    def add_existing_object_update_display(self, existing_obj: CompiledObj) -> None:

        obj_type = existing_obj.obj_type

        if obj_type not in self.cls_to_tracker:
            self.cls_to_tracker[obj_type] = ClassTracker()

        self.cls_to_tracker[obj_type].add_existing(existing_obj)

        self._update_display()

    def close_cls(self, obj_type: str) -> None:
        if obj_type in self.cls_to_tracker:
            self.cls_to_tracker[obj_type].close()
            self._update_display()

    def close(self) -> None:
        self._update_display()
        self.live.stop()

    def _update_display(self) -> Text:
        # self.live.clear()
        text = Text("")

        for obj_type, tracker in self.cls_to_tracker.items():
            text.append(f"\n{obj_type}-s:\n", style="cyan")

            status = tracker.to_status()
            if status:
                text.append(status)

            errors = tracker.to_errors()
            if errors:
                text.append(errors)

            diff = tracker.diff()
            if diff:
                text.append(diff)

        text.append("\n")
        self.live.update(text)
