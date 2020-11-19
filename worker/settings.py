import os
from typing import Final

API_ROOT_URL: Final = os.getenv('API_ROOT_URL', '#SETME')
DATA_API_ROOT_URL: Final = os.getenv('DATA_API_ROOT_URL', '#SETME')
APP_NAME: Final = os.getenv('APP_NAME', '#SETME')
API_KEY: Final = os.getenv('API_KEY', '#SETME')
CACHE_URL: Final[str] = os.getenv('CACHE_URL')
