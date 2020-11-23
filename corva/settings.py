import os
from typing import Final

API_ROOT_URL: Final[str] = os.getenv('API_ROOT_URL')
DATA_API_ROOT_URL: Final[str] = os.getenv('DATA_API_ROOT_URL')
APP_NAME: Final[str] = os.getenv('APP_NAME')
API_KEY: Final[str] = os.getenv('API_KEY')
CACHE_URL: Final[str] = os.getenv('CACHE_URL')
