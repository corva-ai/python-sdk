from typing import Any, Callable, List, Union

REDIS_STORED_VALUE_TYPE = Union[bytes, str, int, float]
SCHEDULED_EVENT_TYPE = List[List[dict]]
STREAM_EVENT_TYPE = List[dict]
TASK_EVENT_TYPE = dict
DISPATCH_TYPE = Callable[['BaseContext', Callable[['BaseContext'], Any]], Any]  # noqa: F821
