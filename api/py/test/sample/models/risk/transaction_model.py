
from ai.chronon.model import Model, ModelType
from ai.chronon.api.ttypes import DataKind, JoinSource, Source, TDataType
from ai.chronon.query import Query, select
from joins.risk.user_transactions import txn_join


"""
This is the "left side" of the join that will comprise our training set. It is responsible for providing the primary keys
and timestamps for which features will be computed.
"""
source = Source(
    joinSource=JoinSource(
        join=txn_join, 
        query=Query(
            selects=select("user_id"),
            )
    ))

v1 = Model(source=source, outputSchema=TDataType(DataKind.DOUBLE), modelType=ModelType.XGBoost)
