# Initialize Demo Data

This directory holds code to setup docker containers for dynamoDB, a spark master, and a spark worker. It also creates a container which contains a parquet table with example data containing anomolies. To start, run:

```
$ cd chronon
$ docker-compose -f docker-init/compose.yaml up --build
...
app-1           | [info] 2024-09-30 20:47:56,448 [main] INFO  play.api.Play - Application started (Prod) (no global state)
app-1           | [info] 2024-09-30 20:47:56,665 [main] INFO  play.core.server.PekkoHttpServer - Listening for HTTP on /[0:0:0:0:0:0:0:0]:9000
```

The **backend** is served at: http://localhost:9000

The **frontend** is served at: http://localhost:3000

You can also access the parquet anomaly data table. To do so, from another terminal run:

`docker-compose exec app bash`

The parquet is available in the /app/data directory.
