from datetime import datetime, timedelta

import fakeredis
import pytest
from freezegun import freeze_time
from redis import ConnectionError, Redis

from corva.cache_adapter import DeprecatedRedisAdapter
from corva.configuration import SETTINGS


@pytest.fixture(scope='function')
def redis_adapter() -> DeprecatedRedisAdapter:
    client = fakeredis.FakeRedis.from_url(url=SETTINGS.CACHE_URL, decode_responses=True)
    return DeprecatedRedisAdapter(hash_name='default_name', client=client)


NAME = 'NAME'
KEY = 'key'
VAL = 'val'
MAPPING = {'key1': 'val1', 'key2': 'val2'}


def test_connect(redis_adapter):
    assert redis_adapter.client.ping()


def test_init_connect_exc():
    fake_cache_url = 'redis://random:123'

    with pytest.raises(ConnectionError):
        DeprecatedRedisAdapter(hash_name='name', client=Redis.from_url(fake_cache_url))


@pytest.mark.parametrize('name', (None, NAME))
def test_hset_and_hget(redis_adapter, name):
    assert redis_adapter.hset(name=name, key=KEY, value=VAL) == 1
    assert redis_adapter.hget(name=name, key=KEY) == VAL


@pytest.mark.parametrize('name', (None, NAME))
def test_hset_mapping_and_hgetall(redis_adapter, name):
    assert redis_adapter.hset(name=NAME, mapping=MAPPING) == len(MAPPING)
    assert redis_adapter.hgetall(name=NAME) == MAPPING


@pytest.mark.parametrize('name', (None, NAME))
def test_hdel_and_exists(redis_adapter, name):
    def exists():
        if name is None:
            return redis_adapter.exists()
        return redis_adapter.exists(name)

    assert redis_adapter.hset(name=name, key=KEY, value=VAL) == 1
    assert exists()
    assert redis_adapter.hdel(keys=[KEY], name=name) == 1
    assert not exists()


@pytest.mark.parametrize('name', (None, NAME))
def test_delete_and_exists(redis_adapter, name):
    def exists():
        if name is None:
            return redis_adapter.exists()
        return redis_adapter.exists(name)

    def delete():
        if name is None:
            return redis_adapter.delete()
        else:
            return redis_adapter.delete(name)

    assert redis_adapter.hset(name=name, key=KEY, value=VAL) == 1
    assert exists()
    assert delete()
    assert not exists()


@pytest.mark.parametrize('name', (None, NAME))
def test_ttl(redis_adapter, name):
    with freeze_time('2020'):
        assert redis_adapter.hset(name=name, key=KEY, value=VAL) == 1
        assert (
            redis_adapter.ttl(name=name) == redis_adapter.DEFAULT_EXPIRY.total_seconds()
        )


@pytest.mark.parametrize('name', (None, NAME))
def test_pttl(redis_adapter, name):
    with freeze_time('2020'):
        assert redis_adapter.hset(name=name, key=KEY, value=VAL) == 1
        assert (
            redis_adapter.pttl(name=name)
            == redis_adapter.DEFAULT_EXPIRY.total_seconds() * 1000
        )


def test_hset_default_expiry(redis_adapter):
    with freeze_time('2020'):
        redis_adapter.hset(key=KEY, value=VAL)
        assert (
            redis_adapter.ttl() == DeprecatedRedisAdapter.DEFAULT_EXPIRY.total_seconds()
        )


def test_hset_expiry_override(redis_adapter):
    with freeze_time('2020'):
        for expiry in [10, 5, 20]:
            redis_adapter.hset(key=KEY, value=VAL, expiry=expiry)
            assert redis_adapter.ttl() == expiry


def test_hset_expiry_disable(redis_adapter):
    with freeze_time('2020'):
        redis_adapter.hset(key=KEY, value=VAL, expiry=5)
        assert redis_adapter.ttl() == 5

        redis_adapter.hset(key=KEY, value=VAL, expiry=None)
        assert redis_adapter.ttl() == -1


def test_hset_expiry(redis_adapter):
    with freeze_time('2020') as frozen_time:
        now = datetime.utcnow()
        redis_adapter.hset(key=KEY, value=VAL, expiry=5)
        frozen_time.move_to(now + timedelta(seconds=5))
        assert redis_adapter.exists()
        frozen_time.move_to(now + timedelta(seconds=5, microseconds=1))
        assert not redis_adapter.exists()
