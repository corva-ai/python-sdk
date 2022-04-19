import datetime
from typing import Iterable

import pytest
import redis

from corva import cache_adapter, configuration

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
) -> Iterable[cache_adapter.RedisRepository]:
    redis_adapter = cache_adapter.RedisRepository(
        hash_name=HASH_NAME, client=redis_client
    )

    yield redis_adapter


class TestSet:
    def test_sets_the_value(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set(key='key', value='value', ttl=1)

        assert redis_client.hgetall(name=redis_adapter.hash_name) == {'key': 'value'}


class TestGet:
    def test_success(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_client.hset(redis_adapter.hash_name, mapping={'k1': 1})
        result = redis_adapter.get('k1')

        assert result == '1'


class TestDelete:
    def test_deletes_the_key(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set(key='key', value='value', ttl=1)
        assert redis_client.hgetall(redis_adapter.hash_name) == {'key': 'value'}
        assert redis_adapter.get(key='key') == 'value'

        redis_adapter.delete(key='key')
        assert redis_adapter.get(key='key') is None

        assert not redis_client.keys(pattern='*')


class TestVacuum:
    def test_vacuum(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
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
        assert set(
            redis_client.zrange(name=redis_adapter.zset_name, start=0, end=-1)
        ) == {'key2', 'key1'}

    def test_does_not_fail_for_empty_cache(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')
        redis_adapter.vacuum(delete_count=1)


class TestTtl:
    def test_returns_nil_for_non_existing_key(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')
        assert redis_adapter.ttl(key='nonexisting') is None

    def test_returns_nil_for_negative_ttl(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        pexpireat = (
            int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
            - 9000
        )
        redis_client.zadd(name=redis_adapter.zset_name, mapping={'key': pexpireat})

        assert redis_adapter.ttl(key='key') is None

    def test_returns_nil_for_zero_ttl(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        pexpireat = (
            int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000) + 0
        )
        redis_client.zadd(name=redis_adapter.zset_name, mapping={'key': pexpireat})

        assert redis_adapter.ttl(key='key') is None

    def test_returns_ttl_for_positive_ttl(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        pexpireat = (
            int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
            + 9000
        )
        redis_client.zadd(name=redis_adapter.zset_name, mapping={'key': pexpireat})

        assert redis_adapter.ttl(key='key') is not None


class TestGetAll:
    def test_1(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        now = (
            int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000) + 0
        )
        past = (
            int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
            - 9000
        )
        future = (
            int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
            + 9000
        )

        redis_client.hset(
            redis_adapter.hash_name,
            mapping={'key1': 1, 'key2': 2, 'key4': 4, 'key5': 5},
        )
        redis_client.zadd(
            name=redis_adapter.zset_name,
            mapping={'key1': now, 'key2': past, 'key3': future, 'key4': future},
        )
        result = redis_adapter.get_all()
        assert result == {'key4': '4', 'key5': '5'}
        # returns not expired, non-existend keys,

    def test_returns_if_no_existing_zset(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_client.hset(redis_adapter.hash_name, mapping={'k': 1})
        result = redis_adapter.get_all()

        assert result == {'k': '1'}

    def test_returns_if_no_entry_in_zset(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_client.zadd(
            name=redis_adapter.zset_name,
            mapping={'notk': 1},
        )
        redis_client.hset(redis_adapter.hash_name, mapping={'k': 1})
        result = redis_adapter.get_all()

        assert result == {'k': '1'}

    def test_returns_if_now_less_than_expireat(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        pexpireat = (
            int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
            + 9000
        )

        redis_client.zadd(
            name=redis_adapter.zset_name,
            mapping={'k': pexpireat},
        )
        redis_client.hset(redis_adapter.hash_name, mapping={'k': 1})
        result = redis_adapter.get_all()

        assert result == {'k': '1'}

    def test_does_not_return_if_now_equal_to_expireat(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        pexpireat = (
            int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000) + 0
        )

        redis_client.zadd(
            name=redis_adapter.zset_name,
            mapping={'k': pexpireat},
        )
        redis_client.hset(redis_adapter.hash_name, mapping={'k': 1})
        result = redis_adapter.get_all()

        assert result == {}

    def test_does_not_return_if_now_bigger_than_expireat(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        pexpireat = (
            int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
            - 9000
        )

        redis_client.zadd(
            name=redis_adapter.zset_name,
            mapping={'k': pexpireat},
        )
        redis_client.hset(redis_adapter.hash_name, mapping={'k': 1})
        result = redis_adapter.get_all()

        assert result == {}

    def test_gets_many(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_client.hset(redis_adapter.hash_name, mapping={'k1': 1, 'k2': 2})
        result = redis_adapter.get_all()

        assert result == {'k1': '1', 'k2': '2'}


class TestGetMany:
    def test_gets_many(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_client.hset(redis_adapter.hash_name, mapping={'k1': 1, 'k2': 2})
        result = redis_adapter.get_many(['k1', 'k2', 'k3'])

        assert result == {'k1': '1', 'k2': '2', 'k3': None}

    def test_gets_if_no_entry_in_zset(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_client.hset(redis_adapter.hash_name, mapping={'k': 1})
        result = redis_adapter.get_many(['k'])

        assert result == {'k': '1'}

    def test_gets_if_now_less_that_expireat(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_client.zadd(
            name=redis_adapter.zset_name,
            mapping={
                'k': int(
                    datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000
                )
                + 9000
            },
        )
        redis_client.hset(redis_adapter.hash_name, mapping={'k': 1})
        result = redis_adapter.get_many(['k'])

        assert result == {'k': '1'}

    def test_returns_nil_if_now_equal_to_expireat(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_client.zadd(
            name=redis_adapter.zset_name,
            mapping={
                'k': int(
                    datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000
                )
                + 0
            },
        )
        redis_client.hset(redis_adapter.hash_name, mapping={'k': 1})
        result = redis_adapter.get_many(['k'])

        assert result == {'k': None}

    def test_returns_nil_if_now_bigger_than_expireat(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_client.zadd(
            name=redis_adapter.zset_name,
            mapping={
                'k': int(
                    datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000
                )
                - 9000
            },
        )
        redis_client.hset(redis_adapter.hash_name, mapping={'k': 1})
        result = redis_adapter.get_many(['k'])

        assert result == {'k': None}


class TestSetMany:
    def test_sets_many(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set_many(data=[('k1', 'v1', 1), ('k2', 'v2', 2)])

        assert redis_client.hgetall(name=redis_adapter.hash_name) == {
            'k1': 'v1',
            'k2': 'v2',
        }

    def test_overwrites_the_value_and_ttl(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set_many(data=[('k', 'v', 1)])
        assert redis_client.ttl(redis_adapter.hash_name) == 1
        assert redis_client.ttl(redis_adapter.zset_name) == 1
        assert redis_client.hgetall(name=redis_adapter.hash_name) == {'k': 'v'}

        redis_adapter.set_many(data=[('k', 'v2', 2)])
        assert redis_client.ttl(redis_adapter.hash_name) == 2
        assert redis_client.ttl(redis_adapter.zset_name) == 2
        assert redis_client.hgetall(name=redis_adapter.hash_name) == {'k': 'v2'}

    def test_expiration(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set_many(data=[('k', 'v', 1)])
        assert redis_client.ttl(redis_adapter.hash_name) == 1
        assert redis_client.ttl(redis_adapter.zset_name) == 1

        redis_adapter.delete(key='k')

        assert not redis_client.keys(pattern='*')

    def test_sets_max_expiration(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set_many(data=[('k1', 'v1', 3), ('k2', 'v2', 1), ('k3', 'v3', 2)])
        assert redis_client.ttl(redis_adapter.hash_name) == 3
        assert redis_client.ttl(redis_adapter.zset_name) == 3

        redis_adapter.set_many(data=[('k4', 'v4', 1)])
        assert redis_client.ttl(redis_adapter.hash_name) == 3  # k1 has max expiration
        assert redis_client.ttl(redis_adapter.zset_name) == 3

        redis_adapter.set_many(data=[('k1', 'v1', 0)])
        assert redis_client.ttl(redis_adapter.hash_name) == 2  # k3 has max expiration
        assert redis_client.ttl(redis_adapter.zset_name) == 2


class TestDeleteMany:
    def test_deletes_the_keys(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set_many([('k1', 'v1', 1), ('k2', 'v2', 1), ('k3', 'v3', 1)])
        assert redis_client.hgetall(redis_adapter.hash_name) == {
            'k1': 'v1',
            'k2': 'v2',
            'k3': 'v3',
        }
        assert redis_adapter.get_many(keys=['k1', 'k2', 'k3']) == {
            'k1': 'v1',
            'k2': 'v2',
            'k3': 'v3',
        }

        redis_adapter.delete_many(keys=['k1', 'k2'])
        assert redis_adapter.get_many(keys=['k1', 'k2', 'k3']) == {
            'k1': None,
            'k2': None,
            'k3': 'v3',
        }

        redis_adapter.delete_many(keys=['k3'])

        assert not redis_client.keys(pattern='*')


class TestDeleteAll:
    def test_delete_all(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set_many([('k1', 'v1', 1), ('k2', 'v2', 1), ('k3', 'v3', 1)])
        assert (
            redis_client.exists(redis_adapter.hash_name, redis_adapter.zset_name) == 2
        )

        redis_adapter.delete_all()
        assert (
            redis_client.exists(redis_adapter.hash_name, redis_adapter.zset_name) == 0
        )
