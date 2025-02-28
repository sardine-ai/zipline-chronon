"""
DO NOT REMOVE

Useful for debugging bazel stuff that goes into path
"""

import sys
import os
print("Current working directory:", os.getcwd())
print("Python path:")
for path in sys.path:
    print("  -", path)

try:
    import ai.chronon.api
    print("Successfully imported ai.chronon.api")
    print("Location:", ai.chronon.api.__file__)
except ImportError as e:
    print("Failed to import ai.chronon.api:", e)