import datetime
from typing import Iterable

import pytest
import redis

from corva import adapters, configuration

HASH_NAME = 'test_hash_name'


@pytest.fixture(scope='function')
def redis_client() -> Iterable[redis.Redis]:
    redis_client = redis.Redis.from_url(
        url=configuration.SETTINGS.CACHE_URL, decode_responses=True
    )

    redis_client.flushall()

    yield redis_client

    redis_client.flushall()


@pytest.fixture(scope='function')
def redis_adapter(
    redis_client: redis.Redis,
) -> Iterable[adapters.cache.RedisRepository]:
    redis_adapter = adapters.cache.RedisRepository(
        hash_name=HASH_NAME, client=redis_client
    )

    yield redis_adapter


class TestSet:
    def test_sets_the_value(
        self,
        redis_client: redis.Redis,
        redis_adapter: adapters.cache.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set(key='key', value='value', ttl=1)

        assert redis_client.hgetall(name=redis_adapter.hash_name) == {'key': 'value'}

    def test_overwrites_the_value_and_ttl(
        self,
        redis_client: redis.Redis,
        redis_adapter: adapters.cache.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set(key='key', value='value', ttl=1)
        assert redis_client.ttl(redis_adapter.hash_name) == 1
        assert redis_client.hgetall(name=redis_adapter.hash_name) == {'key': 'value'}

        redis_adapter.set(key='key', value='value2', ttl=2)
        assert redis_client.ttl(redis_adapter.hash_name) == 2
        assert redis_client.hgetall(name=redis_adapter.hash_name) == {'key': 'value2'}

    def test_expiration(
        self,
        redis_client: redis.Redis,
        redis_adapter: adapters.cache.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set(key='key', value='value', ttl=1)
        assert redis_client.ttl(redis_adapter.hash_name) == 1

        redis_adapter.set(
            key='key',
            value='value',
            ttl=-int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp()) - 9,
        )

        assert not redis_client.keys(pattern='*')

    def test_sets_max_expiration(
        self,
        redis_client: redis.Redis,
        redis_adapter: adapters.cache.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set(key='key1', value='value1', ttl=2)
        assert redis_client.ttl(redis_adapter.hash_name) == 2

        redis_adapter.set(key='key2', value='value2', ttl=1)
        assert redis_client.ttl(redis_adapter.hash_name) == 2  # key1 has max expiration

        redis_adapter.set(key='key1', value='value1', ttl=0)
        assert redis_client.ttl(redis_adapter.hash_name) == 1  # key2 has max expiration

        redis_adapter.set(
            key='key2',
            value='value2',
            ttl=-int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp()) - 9,
        )

        assert not redis_client.keys(pattern='*')


class TestGet:
    def test_gets_if_no_existing_zset(
        self,
        redis_client: redis.Redis,
        redis_adapter: adapters.cache.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_client.hset(name=redis_adapter.hash_name, key='key', value='value')

        assert redis_adapter.get(key='key') == 'value'

    def test_gets_if_no_entry_in_zset(
        self,
        redis_client: redis.Redis,
        redis_adapter: adapters.cache.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_client.zadd(name=redis_adapter.zset_name, mapping={'other_key': 1})
        redis_client.hset(name=redis_adapter.hash_name, key='key', value='value')

        assert redis_adapter.get(key='key') == 'value'

    def test_gets_if_now_less_than_expireat(
        self,
        redis_client: redis.Redis,
        redis_adapter: adapters.cache.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        pexpireat = (
            int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
            + 9000
        )
        redis_client.zadd(name=redis_adapter.zset_name, mapping={'key': pexpireat})
        redis_client.hset(name=redis_adapter.hash_name, key='key', value='value')

        assert redis_adapter.get(key='key') == 'value'

    def test_returns_nil_if_now_equal_to_expireat(
        self,
        redis_client: redis.Redis,
        redis_adapter: adapters.cache.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        expireat = (
            int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000) + 0
        )
        redis_client.zadd(name=redis_adapter.zset_name, mapping={'key': expireat})
        redis_client.hset(name=redis_adapter.hash_name, key='key', value='value')

        assert redis_adapter.get(key='key') is None

    def test_returns_nil_if_now_bigger_than_expireat(
        self,
        redis_client: redis.Redis,
        redis_adapter: adapters.cache.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        expireat = (
            int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
            - 9000
        )
        redis_client.zadd(name=redis_adapter.zset_name, mapping={'key': expireat})
        redis_client.hset(name=redis_adapter.hash_name, key='key', value='value')

        assert redis_adapter.get(key='key') is None


class TestDelete:
    def test_deletes_the_key(
        self,
        redis_client: redis.Redis,
        redis_adapter: adapters.cache.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set(key='key', value='value', ttl=1)
        assert redis_adapter.get(key='key') == 'value'

        redis_adapter.delete(key='key')
        assert redis_adapter.get(key='key') is None

        assert not redis_client.keys(pattern='*')

    def test_delete_makes_key_unavailable(
        self,
        redis_client: redis.Redis,
        redis_adapter: adapters.cache.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set(key='key1', value='value1', ttl=1)
        redis_adapter.set(key='key2', value='value2', ttl=1)
        assert redis_adapter.get(key='key1') == 'value1'

        redis_adapter.delete(key='key1')
        assert redis_adapter.get(key='key1') is None
        assert redis_client.keys(pattern='*')

        redis_adapter.delete(key='key2')

        assert not redis_client.keys(pattern='*')


class TestVacuum:
    def test_vacuum(
        self,
        redis_client: redis.Redis,
        redis_adapter: adapters.cache.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set(key='key1', value='value1', ttl=1)

        redis_adapter.set(key='key2', value='value2', ttl=-1)
        redis_adapter.set(key='key3', value='value3', ttl=-2)

        redis_adapter.vacuum(delete_count=1)
        assert redis_client.hgetall(name=redis_adapter.hash_name) == {
            'key1': 'value1',
            'key2': 'value2',
        }
        assert redis_adapter.get('key3') is None

        redis_adapter.delete(key='key1')

        assert not redis_client.keys(pattern='*')

    def test_does_not_fail_for_empty_cache(
        self,
        redis_client: redis.Redis,
        redis_adapter: adapters.cache.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')
        redis_adapter.vacuum(delete_count=1)
