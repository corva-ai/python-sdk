import datetime

import pydantic_settings
from pydantic import AnyHttpUrl, BeforeValidator, TypeAdapter
from typing_extensions import Annotated


def validate_http_url_to_str(v: str) -> str:
    TypeAdapter(AnyHttpUrl).validate_python(v)
    return v


HttpUrlStr = Annotated[str, BeforeValidator(validate_http_url_to_str)]


class Settings(pydantic_settings.BaseSettings):
    # api
    API_ROOT_URL: HttpUrlStr
    DATA_API_ROOT_URL: HttpUrlStr

    # cache
    CACHE_URL: str
    CACHE_SKIP_MIGRATION: int = 0

    # logger
    LOG_LEVEL: str = 'INFO'
    LOG_THRESHOLD_MESSAGE_SIZE: int = 1000
    LOG_THRESHOLD_MESSAGE_COUNT: int = 15

    # company and app
    APP_KEY: str  # <provider-name-with-dashes>.<app-name-with-dashes>
    PROVIDER: str

    # secrets
    SECRETS_CACHE_TTL: int = int(datetime.timedelta(minutes=5).total_seconds())

    # keep-alive
    POOL_CONNECTIONS_COUNT: int = 20  # Total pools count
    POOL_MAX_SIZE: int = 20  # Max connections count per pool/host
    POOL_BLOCK: bool = True  # Wait until connection released

    # retry
    MAX_RETRY_COUNT: int = 3  # If `0` then retries will be disabled
    BACKOFF_FACTOR: float = 1.0


SETTINGS = Settings()  # type: ignore[call-arg]
