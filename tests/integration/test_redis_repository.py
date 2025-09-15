from typing import Dict, Iterable, Mapping, Sequence

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


class TestGetScript:
    def test_get(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_client.hset(redis_adapter.hash_name, mapping={'k1': 1})

        assert redis_adapter.get('k1') == '1'

    def test_get_all(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_client.hset(redis_adapter.hash_name, mapping={'k1': 1, 'k2': 2})
        result = redis_adapter.get_all()

        assert result == {'k1': '1', 'k2': '2'}

    def test_get_with_empty_keys(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')
        assert redis_adapter.get_all() == {}

    @pytest.mark.parametrize(
        'mapping, keys_to_get, expected',
        [
            pytest.param(
                {'k1': 1, 'k2': 2},
                ['k1', 'k2'],
                {'k1': '1', 'k2': '2'},
                id='Gets many.',
            ),
            pytest.param(
                {'k1': 1},
                ['k2'],
                {'k2': None},
                id='Returns `None` for non-existing keys.',
            ),
        ],
    )
    def test_get_many(
        self,
        mapping: Mapping[str, int],
        keys_to_get: Sequence[str],
        expected: Dict[str, str],
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_client.hset(redis_adapter.hash_name, mapping=mapping)  # type: ignore

        assert redis_adapter.get_many(keys_to_get) == expected

    def test_gets_if_no_entry_in_zset(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_client.hset(redis_adapter.hash_name, mapping={'k': 1})

        assert redis_adapter.get('k') == '1'


class TestSetScript:
    def test_set(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set(key='key', value='value', ttl=1)

        assert redis_client.hgetall(name=redis_adapter.hash_name) == {'key': 'value'}

    def test_set_many(
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

        # TODO: replace code after redis lib will be upgraded with
        #  definition for .httl(...) do migration
        # assert redis_client.httl(redis_adapter.hash_name, "k") == [1]
        assert redis_client.execute_command(
            "HTTL", redis_adapter.hash_name, "FIELDS", 1, "k"
        ) == [1]

        assert redis_client.hgetall(name=redis_adapter.hash_name) == {'k': 'v'}

        redis_adapter.set_many(data=[('k', 'v2', 2)])

        # TODO: replace code after redis lib will be upgraded with
        #  definition for .httl(...) do migration
        # assert redis_client.httl(redis_adapter.hash_name, "k") == [2]
        assert redis_client.execute_command(
            "HTTL", redis_adapter.hash_name, "FIELDS", 1, "k"
        ) == [2]
        assert redis_client.hgetall(name=redis_adapter.hash_name) == {'k': 'v2'}

    def test_expiration(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set_many(data=[('k', 'v', 1)])

        # TODO: replace code after redis lib will be upgraded with
        #  definition for .httl(...) do migration
        # assert redis_client.httl(redis_adapter.hash_name, "k") == [1]

        assert redis_client.execute_command(
            "HTTL", redis_adapter.hash_name, "FIELDS", 1, "k"
        ) == [1]

        redis_adapter.delete(key='k')

        assert not redis_client.keys(pattern='*')

    def test_sets_max_expiration(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set_many(
            data=[
                ('k1', 'v1', 3),
                ('k2', 'v2', 1),
                ('k3', 'v3', 2)
            ]
        )

        assert redis_client.execute_command(
            "HTTL", redis_adapter.hash_name, "FIELDS", 1, "k1"
        ) == [3]

        assert redis_client.execute_command(
            "HTTL", redis_adapter.hash_name, "FIELDS", 1, "k2"
        ) == [1]

        assert redis_client.execute_command(
            "HTTL", redis_adapter.hash_name, "FIELDS", 1, "k3"
        ) == [2]

        # TODO: replace code after redis lib will be upgraded with
        #  definition for .httl(...) do migration
        # assert redis_client.httl(redis_adapter.hash_name, "k1") == [3]
        # assert redis_client.httl(redis_adapter.hash_name, "k2") == [1]
        # assert redis_client.httl(redis_adapter.hash_name, "k3") == [2]


class TestDelete:
    def test_delete(
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

    def test_delete_many(
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


class TestDeleteAllScript:
    def test_delete_all(
        self,
        redis_client: redis.Redis,
        redis_adapter: cache_adapter.RedisRepository,
    ):
        assert not redis_client.keys(pattern='*')

        redis_adapter.set_many([('k1', 'v1', 1), ('k2', 'v2', 1), ('k3', 'v3', 1)])
        assert (
            redis_client.exists(redis_adapter.hash_name) == 1
        )

        redis_adapter.delete_all()
        assert (
            redis_client.exists(redis_adapter.hash_name) == 0
        )
