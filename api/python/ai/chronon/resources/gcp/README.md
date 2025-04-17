
# 🧠 Zipline AI: Sample Chronon Project

This repository demonstrates how to author and run [Chronon](https://chronon.ai) pipelines, including GroupBy and Join definitions, using GCP (BigQuery + Iceberg) as the storage backend.

Chronon is a unified platform for **feature engineering**, enabling **online and offline consistency**, **real-time feature generation**, and **historical backfills** from a single codebase.

---

## 📦 Project Structure

```bash
.
├── group_bys/           # GroupBy definitions (feature aggregations)
├── joins/               # Join definitions (how sources and GroupBys are combined)
├── sources/             # Chronon Source definitions (event tables)
├── compiled/            # Generated configs and outputs
├── teams.py             # Chronon Team configurations
└── README.md
```

---

## 🚀 Quick Start

### 🛠️ Requirements

To get started, make sure you have the following set up:

- ✅ **Python** 3.11 or higher
- ✅ **Zipline CLI** — install via:
  ```bash
  ./zipline-cli-install.sh
- ✅ gcloud CLI — authenticated and configured with the correct GCP project
- ✅ Google Cloud credentials — either:
  - Application Default Credentials (ADC)
  - A service account with access to BigQuery and GCS
- ✅Add this to your shell config (e.g., .bashrc, .zshrc):

```bash
# From the same directory as this README
export PYTHONPATH="$(pwd):$PYTHONPATH"
```

---

## ⚙️ Step-by-step Guide

### 1. Define Your Data Source

Your event source is defined in `sources/data.py`:

```python
source_v1 = Source(events=EventSource(
    table="my_dataset.user_events",
    query=Query(
        selects=selects("user_id", "event_type", "event_timestamp"),
        time_column="event_timestamp",
        partition_column="ds",
    )
))
```

This is a **batch source** defined over a BigQuery table.

---

### 2. Create GroupBy Features

Chronon supports feature aggregation with time windows.

Defined in `group_bys/data.py`:

```python
group_by_v1 = GroupBy(
    backfill_start_date="2023-11-01",
    sources=[source_v1],
    keys=["user_id"],
    online=True,
    aggregations=[
        Aggregation(input_column="purchase_price", operation=Operation.SUM, windows=window_sizes),
        Aggregation(input_column="purchase_price", operation=Operation.COUNT, windows=window_sizes),
        Aggregation(input_column="purchase_price", operation=Operation.AVERAGE, windows=window_sizes),
        Aggregation(input_column="purchase_price", operation=Operation.LAST_K(10)),
    ]
)
```

These features will be computed over 3, 14, and 30-day sliding windows.

---

### 3. Define a Join

In `joins/data.py`, this join combines `source` events with the defined `group_by_v1` features:

```python
v1 = Join(
    left=source,  # Typically your training / scoring events
    right_parts=[JoinPart(group_by=group_by_v1)]
)
```

---

### 4. Configure Your Team

Teams define metadata, Spark config, and environment variables.

In `teams.py`:

```python
default = Team(
    outputNamespace="default",
    conf=ConfigProperties(common={
        "spark.sql.catalog.bigquery_catalog": "ai.chronon.integrations.cloud_gcp.DelegatingBigQueryMetastoreCatalog",
        "spark.sql.catalog.bigquery_catalog.warehouse": "<gcs-prefix>",
      ...
    }),
    env=EnvironmentVariables(common={
        "GCP_PROJECT_ID": "<project-id>",
        ...
    }),
)
```

Make sure to replace placeholders like `<project-id>` and `<gcs-prefix>` with real values.

---

## 🧪 Running a GroupBy Backfill

Run a GroupBy backfill from the CLI:

```bash
zipline run group_by \
  --name group_by_v1 \
  --team default \
  --mode backfill
```

---

## 🔗 Running a Join Job

```bash
zipline run join \
  --name v1 \
  --team default \
  --mode backfill
```

This computes a training set by joining the raw events with the features computed from the GroupBy.

---

## 🧰 Advanced Configuration

You can override config/env vars dynamically via CLI:

```bash
zipline run group_by \
  --name group_by_v1 \
  --team default \
  --mode backfill \
  --set spark.sql.shuffle.partitions=20
```

---

## 📁 Output

Results are written to the configured BigQuery + Iceberg tables under the `outputNamespace` (e.g. `default.group_by_v1` or `default.v1`).

---

## 📚 Resources

- [Chronon Docs](https://chronon.ai)
- [GitHub](https://github.com/airbnb/chronon)
- [Community Slack](https://join.slack.com/t/chrononworkspace/shared_invite/zt-33zbnzwac-ghPZXpYNZJsArXZ5WdBy9g)

---

## 👋 About

This project is a reference scaffold for building scalable feature pipelines using Chronon on GCP. It provides end-to-end visibility from source to production features.