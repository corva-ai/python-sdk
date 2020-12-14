from typing import List, Union

REDIS_STORED_VALUE_TYPE = Union[bytes, str, int, float]
SCHEDULED_EVENT_TYPE = List[List[dict]]
STREAM_EVENT_TYPE = List[dict]
TASK_EVENT_TYPE = dict
EVENT_TYPE = Union[
    SCHEDULED_EVENT_TYPE,
    STREAM_EVENT_TYPE,
    TASK_EVENT_TYPE
]
