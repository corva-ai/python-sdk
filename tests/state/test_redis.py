import json
import time

import pytest

from worker.state.redis import RedisState


def test_connect():
    redis = RedisState()
    assert redis.redis.ping()


def test_connect_exc():
    fake_cache_url = 'redis://random:6379'
    with pytest.raises(ValueError) as exc:
        RedisState(cache_url='redis://random:6379')
    assert str(exc.value) == f'Could not connect to Redis with URL: {fake_cache_url}'


def test_save(redis):
    key = 'key'
    save_state = {'key1': {'nested1': {'nested2': ''}}}
    assert redis.save(state=save_state, state_key=key)
    assert redis.redis.exists(key)
    assert json.loads(redis.redis.get(key)) == save_state
    assert redis.save(state=int(), state_key='random')


def test_save_expire(redis):
    key = 'key'
    save_state = {'key1': {'nested1': {'nested2': ''}}}
    px = 10
    assert redis.save(state=save_state, state_key=key, px=px)
    assert redis.redis.exists(key)
    time.sleep((px+1) / 1000)
    assert not redis.redis.exists(key)


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
