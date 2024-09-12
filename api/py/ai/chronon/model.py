import ai.chronon.api.ttypes as ttypes
import ai.chronon.utils as utils
import logging
import inspect
import json
from typing import List, Optional, Union, Dict, Callable, Tuple

class ModelType:
    XGBoost = ttypes.ModelType.XGBoost
    PyTorth = ttypes.ModelType.PyTorch

# Name must match S3 path that we expose if you're uploading trained models?
def Model(
    source: ttypes.Source,
    outputSchema: dict[str, str],
    modelType: ModelType,
    name: str = None,
    modelParams: dict[str, str] = {}
) -> ttypes.Model:
    outputSchema = None
    # Todo: convert map to Tdata type

    metaData = ttypes.MetaData(
        name=name,
    )

    return ttypes.Model(modelType=modelType, outputSchema=outputSchema, source=source, modelParams=modelParams, metaData=metaData)
