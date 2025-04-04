from typing import List

from rich.text import Text


class DiffResult:

    def __init__(self):
        self.added: List[str] = []
        self.updated: List[str] = []

    def render(self, deleted_names: List[str], indent="  ") -> Text:

        def added_signage():
            return Text("Added", style="dim green")

        def updated_signage():
            return Text("Updated", style="dim yellow")

        def deleted_signage():
            return Text("Deleted", style="red")

        added = [(added_signage(), name) for name in self.added]

        updated = [(updated_signage(), name) for name in self.updated]

        result_order = added + updated

        if deleted_names:
            deleted = [(deleted_signage(), name) for name in deleted_names]
            result_order += deleted

        result_order = sorted(result_order, key=lambda t: t[1])

        text = Text(overflow="fold", no_wrap=False)
        for signage, name in result_order:
            text.append(indent)
            text.append(signage)
            text.append(" ")
            text.append(name)
            text.append("\n")

        if not text:
            return Text(indent + "No new changes detected\n", style="dim")

        return text
