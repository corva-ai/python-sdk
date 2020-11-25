from unittest.mock import patch

import pytest

KWARGS = {'key1': 'val1'}


@pytest.mark.parametrize('call_func_name,mock_func_name', (
     ('store', 'hset'),
     ('load', 'hget'),
     ('loadall', 'hgetall'),
     ('rm', 'hdel'),
     ('ttl', 'ttl'),
     ('pttl', 'pttl'),
))
def test_all(redis, call_func_name, mock_func_name):
    with patch.object(redis.redis, mock_func_name) as mock_func:
        call_func = getattr(redis, call_func_name)
        call_func(**KWARGS)
        mock_func.assert_called_once_with(**KWARGS)


def test_rmall(redis):
    with patch.object(redis.redis, 'delete') as mock_func:
        redis.rmall()
        mock_func.assert_called_once_with()


def test_exists(redis):
    with patch.object(redis.redis, 'exists') as mock_func:
        redis.exists()
        mock_func.assert_called_once_with()
