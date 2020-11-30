from os import getenv
from typing import Final

API_ROOT_URL: Final[str] = getenv('API_ROOT_URL')
DATA_API_ROOT_URL: Final[str] = getenv('DATA_API_ROOT_URL')
APP_KEY: Final[str] = getenv('APP_KEY')
APP_NAME: Final[str] = getenv('APP_NAME')
API_KEY: Final[str] = getenv('API_KEY')

# Logger
LOG_LEVEL: Final[str] = getenv('LOG_LEVEL', 'WARN')
LOG_ASSET_ID: Final[int] = int(getenv('LOG_ASSET_ID', -1))

# Storage
CACHE_URL: Final[str] = getenv('CACHE_URL')
