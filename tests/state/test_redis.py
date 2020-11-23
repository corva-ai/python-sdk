import json
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from redis import ConnectionError

from corva.state.redis import RedisState


def test_connect(redis):
    assert redis.redis.ping()


def test_connect_exc(mock_redis):
    mock_redis.server.connected = False

    fake_cache_url = 'redis://random:6379'

    with pytest.raises(ConnectionError) as exc:
        RedisState(cache_url=fake_cache_url)
    assert str(exc.value) == f'Could not connect to Redis with URL: {fake_cache_url}'


def test_save(redis):
    key = 'key'
    save_state = {'key1': {'nested1': {'nested2': ''}}}
    assert redis.save(state=save_state, state_key=key)
    assert redis.redis.exists(key)
    assert json.loads(redis.redis.get(key)) == save_state
    assert redis.save(state=int(), state_key='random')


def test_save_expire(redis):
    with freeze_time('2020') as frozen_time:
        now = datetime.utcnow()
        key = 'key'
        px = 1
        assert redis.save(state={}, state_key=key, px=px)
        frozen_time.move_to(now + timedelta(milliseconds=px))
        assert redis.redis.exists(key)
        frozen_time.move_to(now + timedelta(milliseconds=px + 1))
        assert not redis.redis.exists(key)


def test_save_json_dumps_exc(redis):
    state = {}
    with patch('corva.state.redis.json.dumps', side_effect=ValueError('')):
        with pytest.raises(ValueError) as exc:
            redis.save(state={}, state_key='key')
        assert str(exc.value) == f'Could not cast state to json: {state}.'


def test_load(redis):
    key = 'key'
    save_state = {'key1': {'nested1': {'nested2': ''}}}
    redis.save(state=save_state, state_key=key)
    assert redis.load(state_key=key) == save_state


def test_load_non_existent_key(redis):
    key = 'key'
    save_state = {'key1': {'nested1': {'nested2': ''}}}
    redis.save(state=save_state, state_key=key)
    assert redis.load(state_key='notkey') == {}


def test_load_zero_value(redis):
    key = 'key'
    save_state = 0
    redis.save(state=save_state, state_key=key)
    assert redis.load(state_key=key) == 0


def test_delete(redis):
    n_delete = 2
    save_state = {'key1': 'val1'}
    keys = ['key1', 'key2', 'key3']
    for key in keys:
        redis.save(state=save_state, state_key=key)
    assert redis.delete(state_keys=keys[:n_delete]) == n_delete
    for key in keys[:n_delete]:
        assert redis.redis.exists(key) == 0
    assert redis.redis.exists(keys[n_delete])  # one key should be left
