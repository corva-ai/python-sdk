from corva.settings import APP_KEY


def get_provider(app_key: str = APP_KEY) -> str:
    return app_key.split('.')[0]
