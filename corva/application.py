from typing import Any, Callable, List, Optional

from corva.models.stream import StreamContext, StreamEvent
from corva.network.api import Api
from corva.settings import CorvaSettings, CORVA_SETTINGS
from corva.stream import stream_runner


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
        self.cache_kwargs = cache_kwargs

        self.settings = CorvaSettings(
            API_ROOT_URL=api_url or CORVA_SETTINGS.API_ROOT_URL,
            DATA_API_ROOT_URL=data_api_url or CORVA_SETTINGS.DATA_API_ROOT_URL,
            API_KEY=api_key or CORVA_SETTINGS.API_KEY,
            CACHE_URL=cache_url or CORVA_SETTINGS.CACHE_URL,
            APP_KEY=app_key or CORVA_SETTINGS.APP_KEY
        )

        self.api = Api(
            api_url=self.settings.API_ROOT_URL,
            data_api_url=self.settings.DATA_API_ROOT_URL,
            api_key=self.settings.API_KEY,
            app_name=self.settings.APP_NAME,
            timeout=api_timeout,
            max_retries=api_max_retries
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

            results.append(stream_runner(fn=fn, context=ctx))

        return results
