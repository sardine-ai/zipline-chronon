# Chronon Quickstart Docker

This directory holds Dockerfiles and compose yamls to bring up the quickstart containers for our various cloud environments.

> **Note**  
> Make sure you have `docker >= 20.10` installed.  
> Install appropriate java, scala, and python versions following the instructions in [devnotes.md](../devnotes.md#install-appropriate-java-scala-and-python-versions).

To start, run:
```bash
$ cd chronon
$ sbt clean assembly
$ docker compose -f quickstart/cloud_gcp/gcp-docker-compose.yml up --build
...
load-serving-data-1  | Metadata load completed successfully!
load-serving-data-1  | Done computing and uploading all serving data to BigTable! 🥳
load-serving-data-1 exited with code 0
```

This compose triggers the spinning up of the fetcher service as well as loading up of groupby upload data + join metadata to the relevant KV store for the chosen cloud env (Bigtable for GCP, DynamoDB for AWS)

At this point, you can curl the fetcher service from your machine (outside Docker) to get the features for a user:
```bash
$ curl -X POST 'http://localhost:9000/v1/fetch/join/quickstart%2Ftraining_set.v2' -H 'Content-Type: application/json' -d '[{"user_id": "5"}]'
...
```

You can also pull up the container shell:
```bash
$ docker compose -f quickstart/cloud_gcp/gcp-docker-compose.yml exec app bash
```
