import datetime
import os

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


def get_test_api_key() -> str:
    """Api key for testing"""

    return os.environ['TEST_API_KEY']


def get_test_bearer() -> str:
    """Bearer token for testing"""

    return os.environ['TEST_BEARER_TOKEN']


def get_test_dataset() -> str:
    """Dataset for testing"""

    return os.environ['TEST_DATASET']


def get_test_company_id() -> int:
    """Company id for testing"""

    return int(os.environ['TEST_COMPANY_ID'])
