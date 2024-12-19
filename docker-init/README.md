# Chronon Observability Docker Demo

This directory holds code to setup docker containers for dynamoDB, a spark master, and a spark worker. It also creates a container which contains a parquet table with example data containing anomolies. To start, run:

> **Note**  
> Make sure you have `docker >= 20.10` installed.  
> Install appropriate java, scala, and python versions following the instructions in [devnotes.md](../devnotes.md#install-appropriate-java-scala-and-python-versions).

```bash
$ cd chronon
$ ./docker-init/build.sh --all
...
app-1           | [info] 2024-09-30 20:47:56,448 [main] INFO  play.api.Play - Application started (Prod) (no global state)
app-1           | [info] 2024-09-30 20:47:56,665 [main] INFO  play.core.server.PekkoHttpServer - Listening for HTTP on /[0:0:0:0:0:0:0:0]:9000
```

The build script will build the relevant Chronon modules (Spark, Frontend, Hub) and then trigger the docker container build. While iterating, you can selectively
build modules you care about to speed your dev loop. For example to only rebuild Spark modules:

```bash
$ ./docker-init/build.sh --spark
Building Spark modules...
...
app-1           | [info] 2024-09-30 20:47:56,448 [main] INFO  play.api.Play - Application started (Prod) (no global state)
app-1           | [info] 2024-09-30 20:47:56,665 [main] INFO  play.core.server.PekkoHttpServer - Listening for HTTP on /[0:0:0:0:0:0:0:0]:9000
```

If you'd like to start up the docker containers without re-building any code:

```bash
$ docker compose -f docker-init/compose.yaml up
```

The **backend** is served at: http://localhost:9000

The **frontend** is served at: http://localhost:3000. This serves statically built code - no live dev server. `cd frontend; pnpm run dev` for a live dev server.

You can also access the parquet anomaly data table. To do so, from another terminal run:

`docker compose exec app bash`

The parquet is available in the /app/data directory.
