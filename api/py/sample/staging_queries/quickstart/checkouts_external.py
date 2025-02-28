#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from gen_thrift.api.ttypes import StagingQuery, MetaData


query = """
SELECT
    ts,
    ds,
    return_id,
    user_id,
    product_id,
    refund_amt
FROM checkouts_external
WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
"""

staging_query = StagingQuery(
    query=query,
    startPartition="2023-10-31",
    metaData=MetaData(name="checkouts_staging_query", outputNamespace="data"),
    setups=[
        "CREATE OR REPLACE TEMPORARY VIEW checkouts_external USING parquet OPTIONS (path 'gs://zl-warehouse/data/checkouts_ds_not_in_parquet/')",
    ],
)
