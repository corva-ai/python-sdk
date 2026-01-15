import datetime
import logging
from typing import Any

from pydantic import AnyHttpUrl, BaseSettings, validator

logger = logging.getLogger("corva")

DEFAULT_MAX_RETRY_COUNT = 3

def _parse_max_retry_count(value: Any) -> int:
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

    logger.warning(
        "Invalid MAX_RETRY_COUNT value %r; using default %d.",
        value,
        DEFAULT_MAX_RETRY_COUNT,
    )
    return DEFAULT_MAX_RETRY_COUNT

def parse_truthy(v):
    if isinstance(v, bool):
        return v
    return str(v or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


class Settings(BaseSettings):
    # api
    API_ROOT_URL: AnyHttpUrl
    DATA_API_ROOT_URL: AnyHttpUrl

    # cache
    CACHE_URL: str

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
    MAX_RETRY_COUNT: int = DEFAULT_MAX_RETRY_COUNT
    BACKOFF_FACTOR: float = 1.0

    OTEL_LOG_SENDING_DISABLED: bool = False

    _validate_otel_log_sending_disabled = validator(
        "OTEL_LOG_SENDING_DISABLED", pre=True, allow_reuse=True
    )(parse_truthy)

    @validator("MAX_RETRY_COUNT", pre=True)
    def parse_max_retry_count(cls, v: Any) -> int:
        return _parse_max_retry_count(v)


SETTINGS = Settings()
