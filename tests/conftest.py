from unittest.mock import patch

import pytest
from fakeredis import FakeRedis

from corva.state.custom_redis import CustomRedis
from corva.state.redis_state import RedisState


@pytest.fixture(scope='function')
def patch_custom_redis():
    """Patches CustomRedis

    1. patches CustomRedis.__bases__ to use FakeRedis instead of Redis
    2. patches redis.from_url with FakeRedis.from_url
    """

    redis_path = 'corva.state.custom_redis'

    redis_state_patcher = patch(f'{redis_path}.CustomRedis.__bases__', (FakeRedis,))
    with redis_state_patcher, \
         patch(f'{redis_path}.from_url', side_effect=FakeRedis.from_url):
        # necessary to stop mock.patch from trying to call delattr when reversing the patch
        redis_state_patcher.is_local = True
        yield


@pytest.fixture(scope='function')
def custom_redis(patch_custom_redis):
    return CustomRedis(default_name='default_name', cache_url='redis://random:6379', decode_responses=True)


@pytest.fixture(scope='function')
def redis(custom_redis):
    return RedisState(redis=custom_redis)
