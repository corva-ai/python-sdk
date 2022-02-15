from typing import Any, Callable
from unittest import mock

from corva import shared
from corva.service.api_sdk import ApiSdkProtocol
from corva.service.cache_sdk import InternalCacheSdkProtocol


def run_app(
    has_secrets: bool,
    app_key: str,
    api_sdk: ApiSdkProtocol,
    cache_sdk: InternalCacheSdkProtocol,
    app: Callable[[], Any],
    vacuum_delete_count: int = 3,
) -> Any:
    cache_sdk.vacuum(delete_count=vacuum_delete_count)

    secrets = api_sdk.get_secrets(app_key=app_key) if has_secrets else {}

    with mock.patch.dict(shared.SECRETS, values=secrets, clear=True):
        result = app()

    return result
