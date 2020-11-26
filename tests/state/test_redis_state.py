from unittest.mock import patch

import pytest

KWARGS = {'key1': 'val1'}
NAMES = ['1', '2']


@pytest.mark.parametrize('call_func_name,mock_func_name', (
     ('store', 'hset'),
     ('load', 'hget'),
     ('load_all', 'hgetall'),
     ('delete', 'hdel'),
     ('ttl', 'ttl'),
     ('pttl', 'pttl'),
))
def test_all(redis, call_func_name, mock_func_name):
    with patch.object(redis.redis, mock_func_name) as mock_func:
        call_func = getattr(redis, call_func_name)
        call_func(**KWARGS)
        mock_func.assert_called_once_with(**KWARGS)


def test_delete_all(redis):
    with patch.object(redis.redis, 'delete') as mock_func:
        redis.delete_all(*NAMES)
        mock_func.assert_called_once_with(*NAMES)


def test_exists(redis):
    with patch.object(redis.redis, 'exists') as mock_func:
        redis.exists(*NAMES)
        mock_func.assert_called_once_with(*NAMES)
