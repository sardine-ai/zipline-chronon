# Initialize Demo Data

This directory holds code to setup docker containers for dynamoDB, a spark master, and a spark worker. It also creates a container which contains a parquet table with example data containing anomolies. To start, run:

```docker-compose up```

To access the parquet table, from another terminal run:

```docker-compose exec app bash```

The parquet is available as data.parquet