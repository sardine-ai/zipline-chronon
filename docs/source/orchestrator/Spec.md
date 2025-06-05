# Unified orchestrator

... is a system that schedules a list of chronon conf-s or queries with the following approach.

- planned
  - each conf (query) that a user writes is planned into a set of nodes. 
  - nodes could be common between queries. we will guarantee that there is no race conditions.
  
- can operate in multiple modes
  - the main modes we support are offline & online
  - the planner creates offline nodes and online nodes from a conf (with potential overlap)
  
- lineage based 
  - nodes are connected to each other - through 
    - output tables they produce - `node.metaData.outputTable` and
    - their table dependencies `node.metaData.executionInfo.tableDependencies`.
  - we maintain a global lineage graph of nodes that we must execute in topological order
  
- partition aware
  - table dependencies on nodes will provide the following details needed to compute a input partition range for a given output partition range
    - startOffset, endOffset, startCutOff, endCutOff
    - formula: inputRange = `PartitionRange(max(output.start - startOffset, startCutOff), )`
  

## Problem statement
