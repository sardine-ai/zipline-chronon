# Python code (use_sql_processor.py)
from py4j.java_gateway import JavaGateway
import ai.chronon.repo.serializer as ser
import ai.chronon.api.ttypes as thrift


def eval(obj, limit=5):
    """
    utility function to run conf's in an interactive environment.

    Open a python cell by typing `#%%` at the beginning of the line
    or open a notebook in vscode and call `eval(obj)` on your conf objects.

    TODO: There is a lot more work to do to make this production ready.
    TODO: The gateway service needs to be able to issue remote jobs and fetch result.
    """

    # Connect to the Gateway Server
    gateway = JavaGateway()

    # Get the evaluator instance
    evaluator = gateway.entry_point

    if isinstance(obj, str):
        return _to_df(evaluator.evalQuery(obj, limit))

    func = None

    if isinstance(obj, thrift.Source):
        func = evaluator.evalSource

    elif isinstance(obj, thrift.GroupBy):
        func = evaluator.evalGroupBy  # TODO

    elif isinstance(obj, thrift.Join):
        func = evaluator.evalJoin  # TODO

    elif isinstance(obj, thrift.StagingQuery):
        func = evaluator.evalStagingQuery  # TODO

    elif isinstance(obj, thrift.Model):
        func = evaluator.evalModel  # TODO

    else:
        raise Exception(f"Unsupported object type for: {obj}")

    thrift_str = ser.thrift_simple_json(obj)

    eval_result = func(thrift_str, limit)

    return _to_df(eval_result)


def _to_df(eval_result):
    from pyspark.sql.dataframe import DataFrame

    error = eval_result.malformedQuery()

    if error:
        raise Exception(error)

    df = eval_result.df()

    py_df = DataFrame(df, df.sparkSession())

    return py_df
