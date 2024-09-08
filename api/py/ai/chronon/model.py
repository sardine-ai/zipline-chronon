import ai.chronon.api.ttypes as ttypes
import ai.chronon.utils as utils
import logging
import inspect
import json
from typing import List, Optional, Union, Dict, Callable, Tuple


def Model(
    name: str,
    join: Union[ttypes.Join | str],
    predictionSchema: map[str, str],
) -> ttypes.Model:
    
    if (isinstance(join, str)):
        # Todo load repo and get Join object
        join=join
    

    return ttypes.model(name=name, join=join, predictionSchema=predictionSchema)

