from unittest.mock import patch

import pytest
from fakeredis import FakeRedis

from corva.state.redis import RedisState


@pytest.fixture(scope='function')
def patch_redis():
    """Patches RedisState

    1. patches RedisState.__bases__ to use FakeRedis instead of Redis
    2. patches redis.from_url with FakeRedis.from_url
    """

    redis_path = 'corva.state.redis'

    redis_state_patcher = patch(f'{redis_path}.RedisState.__bases__', (FakeRedis,))
    with redis_state_patcher, \
         patch(f'{redis_path}.from_url', side_effect=FakeRedis.from_url):
        # necessary to stop mock.patch from trying to call delattr when reversing the patch
        redis_state_patcher.is_local = True
        yield


@pytest.fixture(scope='function')
def redis(patch_redis):
    return RedisState(
        cache_url='redis://random:6379'
    )
