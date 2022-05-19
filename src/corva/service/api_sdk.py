import datetime
from typing import Dict, Optional, Protocol, Tuple

from corva import api


class ApiSdkProtocol(Protocol):
    def get_secrets(self, app_key: str) -> Dict[str, str]:
        ...


class CachingApiSdk:
    SECRETS_CACHE: Dict[str, Tuple[datetime.datetime, Dict[str, str]]] = {}

    def __init__(self, api_sdk: ApiSdkProtocol, ttl: int):
        self.api_sdk = api_sdk
        self.ttl = ttl

    def get_secrets(self, app_key: str) -> Dict[str, str]:
        cache_entry = self.SECRETS_CACHE.get(app_key)

        if cache_entry is not None and cache_entry[0] > datetime.datetime.now(
            tz=datetime.timezone.utc
        ):
            secrets = cache_entry[1]
        else:
            secrets = self.api_sdk.get_secrets(app_key=app_key)
            expireat = datetime.datetime.now(
                tz=datetime.timezone.utc
            ) + datetime.timedelta(seconds=self.ttl)
            self.SECRETS_CACHE[app_key] = (expireat, secrets)

        return secrets


class CorvaApiSdk:
    def __init__(self, api_adapter: api.Api):
        self.api_adapter = api_adapter

    def get_secrets(self, app_key: str) -> Dict[str, str]:
        response = self.api_adapter.get(
            path=f'/v2/apps/secrets/values?app_key={app_key}'
        )
        secrets = response.json()
        return secrets


class FakeApiSdk:
    def __init__(self, secrets: Optional[Dict[str, Dict[str, str]]] = None):
        self.secrets = secrets or {}

    def get_secrets(self, app_key: str) -> Dict[str, str]:
        return self.secrets[app_key]
