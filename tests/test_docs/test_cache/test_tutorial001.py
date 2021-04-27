from pytest_mock import MockerFixture

from corva.models.scheduled import ScheduledEvent
from corva.state.redis_state import RedisState
from docs.src.cache import tutorial001


def test_tutorial001(app_runner, mocker: MockerFixture):
    event = ScheduledEvent(asset_id=0, start_time=0, end_time=0, company_id=0)

    store_spy = mocker.spy(RedisState, 'store')
    load_spy = mocker.spy(RedisState, 'load')
    load_all_spy = mocker.spy(RedisState, 'load_all')

    app_runner(tutorial001.scheduled_app, event)

    assert store_spy.call_count == 5
    assert load_spy.call_count == 4
    assert load_all_spy.call_count == 1
