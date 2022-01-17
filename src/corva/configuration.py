import datetime

import pydantic


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


SETTINGS = Settings()
