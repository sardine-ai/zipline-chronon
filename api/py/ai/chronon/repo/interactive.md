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
export CHRONON_ROOT=/Users/nikhilsimha/repos/etsy/zipline   # choose your conf dir 
```

3. Copy the chronon jar from your blob store and set it to `CHRONON_SPARK_JAR`.
```
# for EXAMPLE, in your ~/.zshrc
export CHRONON_SPARK_JAR=/Users/nikhilsimha/Downloads/chronon_spark_assembly.jar
```


### Running gateway

1. Load parquet files into `$CHRONON_ROOT/local_warehouse/<namespace>/<table>`

2. Start gateway service - from chronon root
```
> ./scripts/interactive/gateway.sh
```

### Using interactively

1. You can interact with your conf objects in vscode in a notebook like so..

```py

# import source, I do this by running cell from the py file actually with "#%%" in vscode.
from group_bys.etsy_search.visit_beacon import source
# import runner
from ai.chronon.repo.interactive import eval

eval(source)
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