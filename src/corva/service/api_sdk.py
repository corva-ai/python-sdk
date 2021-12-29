from typing import Dict, Optional, Protocol

from corva import api


class ApiSdkProtocol(Protocol):
    def get_secrets(self, app_connection_id: int) -> Dict[str, str]:
        ...


class CorvaApiSdk:
    def __init__(self, api_adapter: api.Api):
        self.api_adapter = api_adapter

    def get_secrets(self, app_connection_id: int) -> Dict[str, str]:
        response = self.api_adapter.get(
            path=f'/v2/apps/{app_connection_id}/parameters/values'
        )
        secrets = response.json()
        return secrets


class FakeApiSdk:
    def __init__(self, secrets: Optional[Dict[int, Dict[str, str]]] = None):
        self.secrets = secrets

    def get_secrets(self, app_connection_id: int) -> Dict[str, str]:
        return self.secrets[app_connection_id]
