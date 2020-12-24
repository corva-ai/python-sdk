from typing import Any, Iterator


class Middleware:
    def __init__(self, cls: type, **options: Any):
        self.cls = cls
        self.options = options

    def __iter__(self) -> Iterator:
        as_tuple = (self.cls, self.options)
        return iter(as_tuple)
