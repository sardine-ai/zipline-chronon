

# Why?

  ML, AI & Metric systems alike need to ensure that data that is going in, and coming out is stable - and hence healthy.
Typically, *more effort* is required to build and configure such monitoring systems that the data systems themselves. 
We aim to build an automated, scalable and realtime monitoring platform to *surface data issues* with high signal. 

## Fan-out & Zero-Touch
To simply monitor a single metric or a feature, one typically needs to monitor three separate aspects

- coverage - indicates the ratio of null (absent) to non-null data. Drop is coverage, or increase in null ratio needs 
             to be monitored.  
- distribution - represents the distribution of continuous or discrete value. Changes to these also indicate presence 
                 of issues.
- rate - is global metric - that represents the number of entries entering or exiting the system. 

For complex types - like sequences, vectors and bucketed columns. Monitoring changes to coverage, distribution and rate 
becomes more complex and involved for such types. However, these features also tend to be higher in their importance 
to the model.

Below are some examples 
- Sequences - user action type - impression, click, buy, call for support, refund etc. Sequences are very effective 
              at modeling user intent and are highly applicable to recommendation and fraud detection systems. Typically 
              represented as a variable sized array with potentially nullable elements.
- Bucketed features - are used to analyze more granular *buckets* or facets of an entity. For example - average txn 
                      price per item category - is a map of category string to a floating point value.  
- Vectors - are *summaries* of natural data such as text and images in a high-dimensional space. Useful for searching 
            relevant items or to transfer learning from larger general purpose model to a smaller task specific model.
            Embedding natural data into vectors is on a rapid rise with recent breakthroughs in large foundation 
            modeling techniques.
 
The main takeaway of this section is that typically *the amount of monitoring logic for a data system is a lot more than the 
effort required to implement the processing logic*. This is a big reason why data systems are poorly monitored. Zipline,
because we understand the underlying processing logic, we can automatically derive the monitoring logic - *without any 
additional user configuration*.

## Shift-left
Current generation of monitoring platforms (opensource & proprietary) all directly index raw data and compute monitoring
metrics on page loads or on alert schedules. This means the amount of raw data to index is humongous and wasteful. Since
we know / derive, the exact set of metrics, we can compute them before they ever reach the index. This approach typically
leads to an order-of-magnitude higher scalability and cost efficiency.

## Density
Many features in a model, many metrics to monitor for each feature. Spotting problematic changes from this massive set 
of metrics needs to be explicitly designed for. Designing for signal at such scale - without requiring a lot of user 
clicks and scrolls is very hard. Most solutions are only useful for verifying *known* problems - we want to make our
system truly useful for surfacing *unknown* problems.

## Realtime
While daily drift monitoring is a reasonable starting point. However, for models with potential for financial or impact
or abuse, it is essential that we surface and remedy issues in realtime. 

# Approach
We have three stages of drift computation
- cardinality estimation - to detect different types of features
- summarization - for each feature type, compute appropriate summaries
- drift computation - comparison of summaries to come up with a drift score


## Cardinality estimation
Cardinality is estimated via KLL sketch - which has the most optimal storage to accuracy ratio among all sketching 
methods.

  - scalar columns 
    - high cardinality & numeric => continuous values
    - low cardinality & numeric => discrete values
    - high cardinality & string => textual values
    - low cardinality & string => nominal values
  - array columns
    - fixed length
      - vector values
    - variable length 
      - high cardinality & numeric => array of continuous values
      - low cardinality & numeric => array of discrete values
      - low cardinality & string => array of nominal values
      - high cardinality & string => array of textual values
  - map columns
    - high cardinality & numeric => map of continuous values
    - low cardinality & numeric => map of discrete values 
    - low cardinality & string => map of nominal values
    - high cardinality & string => map of textual values 



notation: b_histogram stands for bounded_histogram 
          f+b_histogram stands for flattened+bounded_histogram

cardinality estimation => detect semantic type
semantic type, data type => prepare func, summary func, drift func

universal => check null ratio drift - avg(if(x is null, 1, 0)) as null_ratio
string, array, map => check length drift - approx_percentile(length(x), (bins), accuracy=10000)
array, map of nums low cardinality=> array_histogram

array => value null ratio drift - sum(transform(col, if(x is null, 1, 0))) as nulls / sum(length(col)) as inner_null_ratio
map => value null ratio drift - sum(transform(map_values(col), if(x is null, 1, 0))) as nulls / sum(length(col)) as inner_null_ratio

low, (nums or string) => (hist:: cast to string, histogram), (psi)
low, (num or string in array) => (hist:: transform(col, x-> cast x as string), array_histogram)
low, (num or string in map) => (hist:: transform(map_values(col), x -> cast x as string), array_histogram)
                               

high, (nums) => (cast to double, kll sketch) (ks, js or psi)
high, string



## Summarization
  - continuous values
  - 
    - short, int -> cast to long, float -> cast to double
        - widen: cast int, short to long or float to double   
        - approx_percentile[0, 0.05, 0.1, ..., 0.95, 1]
        - array of long or double
      - discrete values - map 