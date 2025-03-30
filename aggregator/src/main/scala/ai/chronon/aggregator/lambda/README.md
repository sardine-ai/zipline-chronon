

## Middle-Out Cumulative Summing Algorithm

At a high level we are laying out cumulative sums in a middle-out fashion - from tail to midnight - in batch, and midnight to now in streaming.
- on the batch side - `aggregate: sum_range(window_tail to midnight), time: bucket_start(window_head)`
- on the streaming side - `aggregate: sum_range(max(midnight, window_tail) to now), NO_TIMESTAMP`


while fetching we do two lookups
- from batch     - `key+upload_date: latest_value <= bucket_start(now)`
- from streaming - `key+[upload_date..query_date]: latest_value`

These are both full irs, we are simply going to add them column wise, finalize and return.

There are additional (algorithmic?) concerns specific to creating the batch and streaming aggregations 

### 1. Constructing the batch side aggregates from raw data

They say - "(pseudo) code is worth 10000 words"

We are building up
```
class BatchIr:
    tail: Option[List[IR]]
    middle: Option[IR]
```

```
def build_agg_infos(upload_date, tail_buffer = 2_days)
    
    for agg in aggs:
        if agg.window:            
            agg_info.tail_start = upload_date - agg.window
            agg_info.tail_end = tail_buffer + upload_date - agg.window
        
        agg_info.input_index = schema.index_of(input_column)
        agg_info.agg = agg        
        
        yield agg_info
```    


```
def init_ir(agg_infos, tail_buffer):
    for agg_info in agg_infos:
        if agg_info.window:
            yield BatchIr(tail = null, middle = empty_ir) 
        elif agg_info.window < tail_buffer:
```        
        
def update_ir(ir, row, agg_infos):
    for agg in agg_info:     
    
        tail_ir = ir.empty
        middle_ir = ir.empty
    
        for row in right_range:
            if row.ts > tail_end:
        
        
```

### 2. Constructing the streaming size aggregates

