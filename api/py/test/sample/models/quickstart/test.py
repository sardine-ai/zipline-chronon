
from ai.chronon.model import Model, ModelType
from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select


"""
This is the "left side" of the join that will comprise our training set. It is responsible for providing the primary keys
and timestamps for which features will be computed.
"""
source = Source(
    events=EventSource(
        table="data.checkouts", 
        query=Query(
            selects=select("user_id"), # The primary key used to join various GroupBys together
            time_column="ts",
            ) # The event time used to compute feature values as-of
    ))

v1 = Model(source=source, outputSchema={}, modelType=ModelType.XGBoost)
