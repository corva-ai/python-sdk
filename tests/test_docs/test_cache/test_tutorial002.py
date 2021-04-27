from pytest_mock import MockerFixture

from corva.models.scheduled import ScheduledEvent
from corva.state.redis_state import RedisState
from docs.src.cache import tutorial002


def test_tutorial002(app_runner, mocker: MockerFixture):
    event = ScheduledEvent(asset_id=0, start_time=0, end_time=0, company_id=0)

    store_spy = mocker.spy(RedisState, 'store')
    delete_spy = mocker.spy(RedisState, 'delete')
    delete_all_spy = mocker.spy(RedisState, 'delete_all')
    load_all_spy = mocker.spy(RedisState, 'load_all')

    app_runner(tutorial002.scheduled_app, event)

    assert store_spy.call_count == 1
    assert delete_spy.call_count == 1
    assert delete_all_spy.call_count == 1
    assert load_all_spy.call_count == 2
