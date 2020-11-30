from unittest.mock import patch

import pytest
from fakeredis import FakeRedis

from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState


@pytest.fixture(scope='function')
def patch_redis_adapter():
    """Patches RedisAdapter

    1. patches RedisAdapter.__bases__ to use FakeRedis instead of Redis
    2. patches redis.from_url with FakeRedis.from_url
    """

    redis_adapter_path = 'corva.state.redis_adapter'

    redis_adapter_patcher = patch(f'{redis_adapter_path}.RedisAdapter.__bases__', (FakeRedis,))
    with redis_adapter_patcher, \
         patch(f'{redis_adapter_path}.from_url', side_effect=FakeRedis.from_url):
        # necessary to stop mock.patch from trying to call delattr when reversing the patch
        redis_adapter_patcher.is_local = True
        yield


@pytest.fixture(scope='function')
def redis_adapter(patch_redis_adapter):
    return RedisAdapter(default_name='default_name', cache_url='redis://random:6379', decode_responses=True)


@pytest.fixture(scope='function')
def redis(redis_adapter):
    return RedisState(redis=redis_adapter)
