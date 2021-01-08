from functools import cached_property
from os import getenv
from typing import Optional

from pydantic import BaseSettings


class Settings(BaseSettings):
    class Config:
        keep_untouched = (cached_property,)

    # api
    API_ROOT_URL: Optional[str] = None
    DATA_API_ROOT_URL: Optional[str] = None
    API_KEY: Optional[str] = None

    # cache
    CACHE_URL: Optional[str] = None

    # logger
    LOG_LEVEL: str = 'WARN'

    # misc
    APP_KEY: Optional[str] = None  # <provider>.<app-name-with-dashes>

    @cached_property
    def APP_NAME(self) -> str:
        if app_name := getenv('APP_NAME') is not None:
            return app_name

        app_name_with_dashes = self.APP_KEY.split('.')[1]
        app_name = ' '.join(app_name_with_dashes.split('-')).title()

        return app_name

    @cached_property
    def PROVIDER(self) -> str:
        if provider := getenv('PROVIDER') is not None:
            return provider

        return self.APP_KEY.split('.')[0]


SETTINGS = Settings()
