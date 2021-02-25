import pytest

from corva.api import Api
from corva.configuration import SETTINGS
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState


@pytest.fixture(scope='function')
def redis_adapter(corva_patch):
    return RedisAdapter(default_name='default_name', cache_url=SETTINGS.CACHE_URL)


@pytest.fixture(scope='function')
def redis(redis_adapter):
    return RedisState(redis=redis_adapter)


# TODO: delete after getting rid of corva/app/base.py
@pytest.fixture(scope='function')
def api():
    return Api(
        api_url='https://api.localhost.ai',
        data_api_url='https://data.localhost.ai',
        api_key='',
        app_name='',
    )


class ComparableException(Exception):
    def __eq__(self, other):
        return type(self) is type(other) and self.args == other.args
