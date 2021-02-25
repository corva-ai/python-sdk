import pydantic


class Settings(pydantic.BaseSettings):
    # api
    API_ROOT_URL: pydantic.AnyHttpUrl
    DATA_API_ROOT_URL: pydantic.AnyHttpUrl

    # cache
    CACHE_URL: str

    # logger
    LOG_LEVEL: str = 'WARN'

    # company and app
    APP_KEY: str  # <provider-name-with-dashes>.<app-name-with-dashes>
    PROVIDER: str

    @property
    def APP_NAME(self) -> str:
        app_name_with_dashes = self.APP_KEY.split('.')[1]
        app_name = app_name_with_dashes.replace('-', ' ').title()

        return app_name


SETTINGS = Settings()
