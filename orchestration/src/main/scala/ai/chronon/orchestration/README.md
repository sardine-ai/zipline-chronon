# Global planner for chronon workloads

We will currently focus only on batch workloads

## Batch 

We have a few stages of logic in this pipeline. 

> NOTE: `>>` represents dependency operator. `a >> b` means b depends on a.

1. parse confs 
   1. walk the directory structure of chronon and parse configs => logical nodes
   
2. convert logical nodes to physical nodes
   1. Foreach groupBy - if coalescing is enabled
      1. insert groupBy's into `coalesced = index[conf.source + conf.key]`
      2. compare previous coalesced to new coalesced and do tetris backfill
   2. Foreach groupBy
      1. if fct + batch
         1. logical nodes are - partition_ir >> snapshot (== backfill) >> upload  
      2. if fct + realtime
         1. logical nodes are - partition_ir (collapsed) + raw (tails) >> sawtooth_ir 
         2. sawtooth_ir >> uploads (if online = True)
         3. sawtooth_ir >> snapshots (if backfill = true)
      3. if dim + batch
         1. can't cache - raw >> snapshot, snapshot >> upload
      4. if dim + realtime
         1. raw >> sawtooth_ir 
      5. scd works similar to fct
   3. For each join
      1. bootstrap_source(s) >> joinPart tetris   
      2. (source_hash + group_by) >> joinPart
      
      3. left-fct
         1. gb-case::(dim/fct batch) snapshot >> joinPart   
         2. gb-case::(fct realtime) sawtooth + ds_raw >> joinPart
         3. gb-case::(dim realtime) sawtooth + ds_mutations >> joinPart
      4. left-dim
         1. gb-case::dim/fct snapshot >> joinPart
      5. left-scd is sub-optimal - we ask to use staging query to convert to fct
      
      6. bootstrap >> missingPartRanges >> joinParts >> final join
      7. join_raw >> derivations (view?)
      8. label source + left >> label_part
      9. label_part + join_raw >> final join
      
3. workflow
   1. trigger staging query submissions on submit
   2. trigger gb_snapshot & sawtooth_ir computations on submit
   3. trigger join computations on schedule or on-demand
