from typing import Callable, Optional

from corva.models.base import BaseContext
from corva.network.api import Api


def init_api_factory(
     *,
     api_url: str,
     data_api_url: str,
     api_key: str,
     api_name: str,
     timeout: Optional[int] = None,
     max_retries: Optional[int] = None
) -> Callable:
    def init_api(context: BaseContext, call_next: Callable) -> BaseContext:
        kwargs = dict(api_url=api_url, data_api_url=data_api_url, api_key=api_key, api_name=api_name)
        if timeout is not None:
            kwargs['timeout'] = timeout
        if max_retries is not None:
            kwargs['max_retries'] = max_retries

        context.api = Api(**kwargs)

        context = call_next(context)

        return context

    return init_api
