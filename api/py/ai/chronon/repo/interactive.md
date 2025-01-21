## What?

Interactive runner for chronon python objects.
Currently only sources are supported.

There could be a longer roadmap where we host the evaluator as a service that pulls sample rows from remote warehouse (bigquery etc) and caches.  

Once we bulk it out - we can use this to power 
- Iterate on joins, staging_queries, groupBys, sources, models etc.
- debug data of a particular key
- even issue full scale jobs to our orchestrator and get back a progress bar directly into notebook.

### Setup


1. Install spark (and java if you don't have it already)
```
> wget  https://www.apache.org/dyn/closer.lua/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz
> tar -xzf spark-3.5.4-bin-hadoop3.tgz
``` 

2. in your `~/.bashrc` or `~/.zshrc` add `SPARK_HOME` & `CHRONON_ROOT` env vars
```
export SPARK_HOME=/Users/nikhilsimha/spark-3.5.4-bin-hadoop3                      # choose dir where you unpacked spark
export CHRONON_ROOT=/Users/nikhilsimha/repos/chronon/api/py/test/sample   # choose your conf dir 
```

### Running gateway

1.  Load parquet files into `$CHRONON_ROOT/local_warehouse/<namespace>/<table>`


2. Start gateway service - from chronon root
```
> sbt spark/assembly && ./scripts/interactive/gateway.sh
```

### Using interactively

1. You can interact with your conf objects in vscode in a notebook like so..

```py
from group_bys.our clients_search.visit_beacon import source
from ai.chronon.repo.interactive import eval

df = eval(source)
df.show()
```

```
+-------------+--------------------+----------+
|           ts|          event_name|listing_id|
+-------------+--------------------+----------+
|1736895673581|          login_view|      NULL|
|1736895673582|gdpr_consent_prom...|      NULL|
|1736895673959|login_view_comple...|      NULL|
|1736895674332|transcend_cmp_air...|      NULL|
|1736895678352|       signin_submit|      NULL|
+-------------+--------------------+----------+
```