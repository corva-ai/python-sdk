import os
from typing import Final


API_ROOT_URL: Final[str] = os.getenv('API_ROOT_URL')
DATA_API_ROOT_URL: Final[str] = os.getenv('DATA_API_ROOT_URL')
APP_KEY: Final[str] = os.getenv('APP_KEY')
APP_NAME: Final[str] = os.getenv('APP_NAME')
API_KEY: Final[str] = os.getenv('API_KEY')
CACHE_URL: Final[str] = os.getenv('CACHE_URL')
LOG_LEVEL: Final[str] = os.getenv('LOG_LEVEL', 'WARN')
LOG_ASSET_ID: Final[int] = int(os.getenv('LOG_ASSET_ID', -1))
