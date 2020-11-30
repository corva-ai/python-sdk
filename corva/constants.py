from typing import List, Union


REDIS_STORED_VALUE_TYPE = Union[bytes, str, int, float]
EVENT_TYPE = Union[
    List[List[dict]],  # scheduled event
    List[dict]  # stream event
]
