import contextlib
import datetime
import time

import freezegun
import pytest
from pytest_mock import MockerFixture

from corva import ScheduledDataTimeEvent
from corva.state.redis_state import RedisState
from docs.src.cache import tutorial001, tutorial002, tutorial003


def test_tutorial001(app_runner, mocker: MockerFixture):
    event = ScheduledDataTimeEvent(asset_id=0, start_time=0, end_time=0, company_id=0)

    store_spy = mocker.spy(RedisState, 'store')
    load_spy = mocker.spy(RedisState, 'load')
    load_all_spy = mocker.spy(RedisState, 'load_all')

    app_runner(tutorial001.scheduled_app, event)

    assert store_spy.call_count == 5
    assert load_spy.call_count == 4
    assert load_all_spy.call_count == 1


def test_tutorial002(app_runner, mocker: MockerFixture):
    event = ScheduledDataTimeEvent(asset_id=0, start_time=0, end_time=0, company_id=0)

    store_spy = mocker.spy(RedisState, 'store')
    delete_spy = mocker.spy(RedisState, 'delete')
    delete_all_spy = mocker.spy(RedisState, 'delete_all')
    load_all_spy = mocker.spy(RedisState, 'load_all')

    app_runner(tutorial002.scheduled_app, event)

    assert store_spy.call_count == 1
    assert delete_spy.call_count == 1
    assert delete_all_spy.call_count == 1
    assert load_all_spy.call_count == 2


@pytest.mark.parametrize(
    'delta,ctx',
    (
        [datetime.timedelta(seconds=60, microseconds=1), contextlib.nullcontext()],
        [datetime.timedelta(seconds=60), pytest.raises(AssertionError)],
    ),
)
def test_tutorial003(delta: datetime.timedelta, ctx, app_runner, mocker: MockerFixture):
    event = ScheduledDataTimeEvent(asset_id=0, start_time=0, end_time=0, company_id=0)

    store_spy = mocker.spy(RedisState, 'store')
    ttl_spy = mocker.spy(RedisState, 'ttl')
    pttl_spy = mocker.spy(RedisState, 'pttl')
    exists_spy = mocker.spy(RedisState, 'exists')

    time_to_freeze = datetime.datetime(year=2021, month=1, day=1)
    with freezegun.freeze_time(time_to_freeze) as frozen_time:
        mocker.patch.object(
            time,
            'sleep',
            lambda *args, **kwargs: frozen_time.move_to(time_to_freeze + delta),
        )
        with ctx:
            app_runner(tutorial003.scheduled_app, event)

    assert store_spy.call_count == 2
    assert ttl_spy.call_count == 1
    assert pttl_spy.call_count == 1
    assert exists_spy.call_count == 2
