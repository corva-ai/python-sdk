from typing import Any, Callable, TypeVar, Union

_T = TypeVar('_T')
REDIS_STORED_VALUE_TYPE = Union[bytes, str, int, float]
MIDDLEWARE_CALL_TYPE = Callable[[_T], Any]
MIDDLEWARE_TYPE = Callable[[_T, MIDDLEWARE_CALL_TYPE], Any]
