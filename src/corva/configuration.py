import datetime

import pydantic_settings
from pydantic import AnyHttpUrl, BeforeValidator, TypeAdapter
from typing_extensions import Annotated


def validate_http_url_to_str(v: str) -> str:
    TypeAdapter(AnyHttpUrl).validate_python(v)
    return v


def _parse_max_retry_count(value) -> int:
    from .logger import CORVA_LOGGER

    if value is None or value == "":
        return DEFAULT_MAX_RETRY_COUNT
    try:
        if isinstance(value, bool):
            raise TypeError
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            return int(value.strip())
    except (TypeError, ValueError):
        pass

    CORVA_LOGGER.warning(
        "Invalid MAX_RETRY_COUNT value %r; using default %d.",
        value,
        DEFAULT_MAX_RETRY_COUNT,
    )
    return DEFAULT_MAX_RETRY_COUNT


HttpUrlStr = Annotated[str, BeforeValidator(validate_http_url_to_str)]
MaxRetryValidator = Annotated[int, BeforeValidator(lambda v: _parse_max_retry_count(v))]

DEFAULT_MAX_RETRY_COUNT = 3


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

    # retry. If `0` then retries will be disabled
    MAX_RETRY_COUNT: MaxRetryValidator = DEFAULT_MAX_RETRY_COUNT
    BACKOFF_FACTOR: float = 1.0

    # OTEL
    OTEL_LOG_SENDING_DISABLED: bool = False


SETTINGS = Settings()  # type: ignore[call-arg]
