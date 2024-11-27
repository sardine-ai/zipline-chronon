# Populate Observability Demo Data
To populate the observability demo data:
* Launch the set of docker containers:
```bash
~/workspace/chronon $ docker-compose -f docker-init/compose.yaml up --build
...
app-1           | [info] 2024-11-26 05:10:45,758 [main] INFO  play.api.Play - Application started (Prod) (no global state)
app-1           | [info] 2024-11-26 05:10:45,958 [main] INFO  play.core.server.AkkaHttpServer - Listening for HTTP on /[0:0:0:0:0:0:0:0]:9000
```
(you can skip the --build if you don't wish to rebuild your code)

Now you can trigger the script to load summary data:
```bash
~/workspace/chronon $ docker-init/demo/load_summaries.sh
...
Done uploading summaries! 🥳
```

# Streamlit local experimentation
run build.sh once, and you can repeatedly exec to quickly visualize 

In first terminal: `sbt spark/assembly` 
In second terminal: `./run.sh` to load the built jar and serve the data on localhost:8181
In third terminal: `streamlit run viz.py`

