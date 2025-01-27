from unittest.mock import patch

import pytest

from corva.configuration import SETTINGS
from corva.service.cache_sdk import UserRedisSdk


@pytest.fixture(scope='function')
def redis() -> UserRedisSdk:
    return UserRedisSdk(
        hash_name='test_hash_name',
        redis_dsn=SETTINGS.CACHE_URL,
        use_fakes=True,
    )


KWARGS = {'key1': 'val1'}
NAMES = ['1', '2']


@pytest.mark.parametrize(
    'call_func_name,mock_func_name',
    (
        ('store', 'hset'),
        ('load', 'hget'),
        ('load_all', 'hgetall'),
        ('ttl', 'ttl'),
        ('pttl', 'pttl'),
    ),
)
def test_all(redis, call_func_name, mock_func_name):
    with patch.object(redis.old_cache_repo, mock_func_name) as mock_func:
        call_func = getattr(redis, call_func_name)
        call_func(**KWARGS)
        mock_func.assert_called_once_with(**KWARGS)


def test_delete_all(redis):
    with patch.object(redis.old_cache_repo, 'delete') as mock_func:
        redis.delete_all(*NAMES)
        mock_func.assert_called_once_with(*NAMES)


def test_delete(redis):
    with patch.object(redis.old_cache_repo, 'hdel') as mock_func:
        redis.delete(['1'])
        mock_func.assert_called_once_with(keys=['1'], name=None)


def test__user_redis_cache__main_methods_works(redis):
    redis.set(key='key1', value='text1')
    redis.set(key='key2', value='text2')
    redis.set(key='key3', value='text3')

    assert redis.get(key='key1') == 'text1'
    assert redis.get_many(keys=['key2', 'key3']) == {'key2': 'text2', 'key3': 'text3'}

    redis.delete(key='key1')
    assert redis.get(key='key1') is None

    redis.delete_many(keys=['key2', 'key3'])
    assert redis.get_many(keys=['key2', 'key3']) == {'key2': None, 'key3': None}


def test_exists(redis):
    with patch.object(redis.old_cache_repo, 'exists') as mock_func:
        redis.exists(*NAMES)
        mock_func.assert_called_once_with(*NAMES)
