from typing import Optional

import pydantic


class Settings(pydantic.BaseSettings):
    # api
    API_ROOT_URL: Optional[pydantic.AnyHttpUrl] = None
    DATA_API_ROOT_URL: Optional[pydantic.AnyHttpUrl] = None

    # cache
    CACHE_URL: Optional[str] = None

    # logger
    LOG_LEVEL: str = 'WARN'

    APP_KEY: Optional[str] = None  # <provider-name-with-dashes>.<app-name-with-dashes>
    PROVIDER: Optional[str] = None

    @property
    def APP_NAME(self) -> Optional[str]:
        if self.APP_KEY is None:
            return None

        app_name_with_dashes = self.APP_KEY.split('.')[1]
        app_name = app_name_with_dashes.replace('-', ' ').title()

        return app_name


SETTINGS = Settings()
