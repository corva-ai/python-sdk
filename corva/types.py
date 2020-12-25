from typing import Any, Callable, List, Union

REDIS_STORED_VALUE_TYPE = Union[bytes, str, int, float]
DISPATCH_TYPE = Callable[['BaseContext', Callable[['BaseContext'], Any]], Any]  # noqa: F821
