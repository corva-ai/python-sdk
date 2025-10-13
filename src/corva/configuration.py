import datetime

import pydantic


def parse_truthy(v):
    if isinstance(v, bool):
        return v
    return str(v or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


class Settings(pydantic.BaseSettings):
    # api
    API_ROOT_URL: pydantic.AnyHttpUrl
    DATA_API_ROOT_URL: pydantic.AnyHttpUrl

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

    # retry
    MAX_RETRY_COUNT: int = 3  # If `0` then retries will be disabled
    BACKOFF_FACTOR: float = 1.0

    OTEL_LOG_SENDING_DISABLED: bool = False

    _validate_otel_log_sending_disabled = pydantic.validator(
        "OTEL_LOG_SENDING_DISABLED", pre=True, allow_reuse=True
    )(parse_truthy)


SETTINGS = Settings()
