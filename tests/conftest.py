import pytest

from worker.state.redis import RedisState


@pytest.fixture(scope='function')
def redis():
    return RedisState()


@pytest.fixture(scope='function', autouse=True)
def flush_redis(redis):
    redis.redis.flushall()
    yield
    redis.redis.flushall()
