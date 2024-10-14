#!/bin/bash
if ! python3.8 generate_anomalous_data.py; then
    echo "Error: Failed to generate anomalous data" >&2
    exit 1
fi

# Add these java options as without them we hit the below error:
# throws java.lang.ClassFormatError accessible: module java.base does not "opens java.lang" to unnamed module @36328710
export JAVA_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
exec "./hub/bin/hub"
