import functools
import inspect
from types import SimpleNamespace
from typing import Any, Callable, ClassVar, Dict, Optional, Union

from corva.api import Api
from corva.configuration import SETTINGS
from corva.models.scheduled.scheduled import ScheduledEvent
from corva.models.stream.stream import StreamEvent
from corva.models.task import TaskEvent
from corva.service import service
from corva.service.api_sdk import FakeApiSdk
from corva.service.cache_sdk import FakeInternalCacheSdk, UserRedisSdk


class TestClient:
    """Interface for testing Corva apps.

    Attributes:
        _context: AWS Lambda context expected by Corva.
        _api: Api instance.
    """

    _context: ClassVar[SimpleNamespace] = SimpleNamespace(
        aws_request_id='qwerty', client_context=SimpleNamespace(env={'API_KEY': '123'})
    )
    _api: ClassVar[Api] = Api(
        api_url=SETTINGS.API_ROOT_URL,
        data_api_url=SETTINGS.DATA_API_ROOT_URL,
        api_key=_context.client_context.env['API_KEY'],
        app_key=SETTINGS.APP_KEY,
    )

    @staticmethod
    def run(
        fn: Callable,
        event: Union[TaskEvent, StreamEvent, ScheduledEvent],
        *,
        secrets: Optional[Dict[str, str]] = None,
        cache: Optional[UserRedisSdk] = None
    ) -> Any:

        app: Callable[[], Any]

        if isinstance(event, TaskEvent):
            app = functools.partial(inspect.unwrap(fn), event, TestClient._api)

        if isinstance(event, (ScheduledEvent, StreamEvent)):
            if not cache:
                cache = UserRedisSdk(
                    hash_name="hash_name",
                    redis_dsn=SETTINGS.CACHE_URL,
                    use_fakes=True,
                )

            app = functools.partial(
                inspect.unwrap(fn),
                event,
                TestClient._api,
                cache,
            )

        return service.run_app(
            has_secrets=True,
            app_key='test_app_key',
            api_sdk=FakeApiSdk(secrets={'test_app_key': secrets or {}}),
            cache_sdk=FakeInternalCacheSdk(),
            app=app,
        )
