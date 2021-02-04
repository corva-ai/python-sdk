from os import getenv
from typing import Optional

import pydantic


class Settings(pydantic.BaseSettings):
    # api
    API_ROOT_URL: Optional[pydantic.AnyHttpUrl] = None
    DATA_API_ROOT_URL: Optional[pydantic.AnyHttpUrl] = None
    API_KEY: Optional[str] = None

    # cache
    CACHE_URL: Optional[str] = None

    # logger
    LOG_LEVEL: str = 'WARN'

    APP_KEY: Optional[str] = None  # <provider>.<app-name-with-dashes>

    @property
    def APP_NAME(self) -> str:
        if (app_name := getenv('APP_NAME')) is not None:
            return app_name

        app_name_with_dashes = self.APP_KEY.split('.')[1]
        app_name = app_name_with_dashes.replace('-', ' ').title()

        return app_name

    @property
    def PROVIDER(self) -> str:
        if (provider := getenv('PROVIDER')) is not None:
            return provider

        return self.APP_KEY.split('.')[0]


SETTINGS = Settings()
