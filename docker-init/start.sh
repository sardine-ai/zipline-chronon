#!/bin/bash
if ! python3 generate_anomalous_data.py; then
    echo "Error: Failed to generate anomalous data" >&2
    exit 1
fi

exec "./hub/bin/hub"
