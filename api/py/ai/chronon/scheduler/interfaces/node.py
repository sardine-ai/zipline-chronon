class Node:
    def __init__(self, name, command, *args, **kwargs):
        self.name = name
        self.command = command
        self.args = args
        self.kwargs = kwargs
        self.dependencies = []

    def add_dependency(self, node):
        self.dependencies.append(node)

    def to_dict(self):
        """Convert the Node instance into a dictionary for JSON serialization."""
        return {
            "name": self.name,
            "command": self.command,
            "args": self.args,
            "kwargs": self.kwargs,
            "dependencies": [dep.name for dep in self.dependencies]
        }
