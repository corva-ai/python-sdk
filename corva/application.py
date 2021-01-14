from typing import Any, Callable, List, Optional

from corva.models.stream import StreamContext, StreamEvent
from corva.network.api import Api
from corva.settings import SETTINGS
from corva.stream import stream


class Corva:
    def __init__(
         self,
         api_url: Optional[str] = None,
         data_api_url: Optional[str] = None,
         cache_url: Optional[str] = None,
         api_key: Optional[str] = None,
         app_key: Optional[str] = None,
         api_timeout: Optional[int] = None,
         api_max_retries: Optional[int] = None,
         cache_kwargs: Optional[dict] = None
    ):
        self.settings = SETTINGS.copy()
        self.cache_kwargs = cache_kwargs

        if api_url is not None:
            self.settings.API_ROOT_URL = api_url
        if data_api_url is not None:
            self.settings.DATA_API_ROOT_URL = data_api_url
        if cache_url is not None:
            self.settings.CACHE_URL = cache_url
        if api_key is not None:
            self.settings.API_KEY = api_key
        if app_key is not None:
            self.settings.APP_KEY = app_key

        api_kwargs = {}
        if api_timeout is not None:
            api_kwargs['timeout'] = api_timeout
        if api_max_retries is not None:
            api_kwargs['max_retries'] = api_max_retries
        self.api = Api(
            api_url=self.settings.API_ROOT_URL,
            data_api_url=self.settings.DATA_API_ROOT_URL,
            api_key=self.settings.API_KEY,
            app_name=self.settings.APP_NAME,
            **api_kwargs
        )

    def stream(
         self,
         fn: Callable,
         event: str,
         *,
         filter_by_timestamp: bool = False,
         filter_by_depth: bool = False
    ) -> List[Any]:
        events = StreamEvent.from_raw_event(event=event, app_key=self.settings.APP_KEY)

        results = []

        for event in events:
            ctx = StreamContext(
                event=event,
                settings=self.settings,
                api=self.api,
                cache_kwargs=self.cache_kwargs,
                filter_by_timestamp=filter_by_timestamp,
                filter_by_depth=filter_by_depth
            )

            results.append(stream(fn=fn, context=ctx))

        return results
