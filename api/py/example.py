def GroupBy(*args):
    pass

def EntitySource(*args):
    pass
def Query(*args):
    pass
selects=Query
Aggregation=Query
Join=Query
JoinPart=Query
AVERAGE=1
VARIANCE=1

def build_group_by(*key_columns):
    return GroupBy(
        sources=[
            EntitySource(
                snapshotTable="payments.transactions",  # hive daily table snapshot
                mutationsTable="payments.daily_transaction_mutations",  # hive mutations log
                mutationsTopic="payments.transaction_mutations",  # kafka mutation events
                query=Query(
                    selects=selects("amount_usd"),
                    wheres=["amount_usd > 0", "transaction_status = 'SUCCESSFUL'"]
                )
            )
        ],
        keys=key_columns,
        aggregations=[
            Aggregation(
                operation=op, 
                input_column="amount_usd", 
                windows=["30d"]
            ) for op in (AVERAGE, VARIANCE)
        ]
    )
user_txn_features = build_group_by("user")
merchant_txn_features = build_group_by("merchant")
interaction_txn_features = build_group_by("user", "merchant")

txn_features = Join(
    # keys are automatically mapped from left to right_parts
    right_parts=[
        JoinPart(groupBy=user_txn_features),
        JoinPart(groupBy=merchant_txn_features),
        JoinPart(groupBy=interaction_txn_features),
    ],
    derivations={
        f"{name}_z_score": f"(amount_usd - {name}_txn_features_amount_usd_average_30d)/"+
            "{name}_txn_features_amount_usd_variance_30d"
        for name in ("user", "merchant", "interaction")
    }
)


from abc import ABC, abstractmethod
from ai.chronon.api.ttypes import TDataType
from dataclasses import dataclass

class Int:
    pass


@dataclass 
class Table1:
    person: Int




    

class Expr:
    def __init__(self):
        pass
    
    @abstractmethod
    def ttype(self) -> TDataType:
        pass

    @abstractmethod
    def print(self) -> str:
        pass


class ArrayExpr(Expr):

    def __init__(self, )


def array(*args): Expr