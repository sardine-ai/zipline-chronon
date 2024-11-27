run build.sh once, and you can repeatedly exec to quickly visualize 

In first terminal: `sbt spark/assembly` 
In second terminal: `./run.sh` to load the built jar and serve the data on localhost:8181
In third terminal: `streamlit run viz.py`
