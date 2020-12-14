import pytest
from pytest_mock import MockerFixture

from corva.models.scheduled import ScheduledContext, ScheduledEventData
from corva.app.scheduled import ScheduledApp
from corva.event import Event
from tests.conftest import APP_KEY, CACHE_URL


@pytest.fixture(scope='function')
def scheduled_app(api):
    return ScheduledApp(api=api, app_key=APP_KEY, cache_url=CACHE_URL)


@pytest.fixture(scope='module')
def scheduled_event_data_factory():
    def _scheduled_event_data_factory(**kwargs):
        default_kwargs = {
            'cron_string': str(),
            'environment': str(),
            'app': int(),
            'app_key': str(),
            'app_version': None,
            'app_connection_id': int(),
            'app_stream_id': int(),
            'source_type': str(),
            'company': int(),
            'provider': str(),
            'schedule': int(),
            'interval': int(),
            'schedule_start': int(),
            'schedule_end': int(),
            'asset_id': int(),
            'asset_name': str(),
            'asset_type': str(),
            'timezone': str(),
            'log_type': str()
        }
        default_kwargs.update(kwargs)

        return ScheduledEventData(**default_kwargs)

    return _scheduled_event_data_factory


@pytest.fixture(scope='function')
def scheduled_context_factory(scheduled_event_data_factory, redis):
    def _scheduled_context_factory(**kwargs):
        default_params = {
            'event': Event([scheduled_event_data_factory()]),
            'state': redis
        }
        default_params.update(kwargs)

        return ScheduledContext(**default_params)

    return _scheduled_context_factory


def test_group_by_field():
    assert ScheduledApp.group_by_field == 'app_connection_id'


def test_post_process(
     mocker: MockerFixture, scheduled_app, scheduled_event_data_factory, scheduled_context_factory
):
    event = Event([scheduled_event_data_factory(schedule=1), scheduled_event_data_factory(schedule=2)])
    context = scheduled_context_factory(event=event)

    update_schedule_status_mock = mocker.patch.object(scheduled_app, 'update_schedule_status')

    scheduled_app.post_process(context=context)

    assert update_schedule_status_mock.call_count == len(event)
    update_schedule_status_mock.assert_has_calls(
        [
            mocker.call(schedule=1, status='completed'),
            mocker.call(schedule=2, status='completed')
        ]
    )


def test_update_schedule_status(mocker: MockerFixture, scheduled_app):
    schedule = 1
    status = 'status'

    mocker.patch.object(scheduled_app.api.session, 'request')
    post_spy = mocker.patch.object(scheduled_app.api, 'post')

    scheduled_app.update_schedule_status(schedule=schedule, status=status)

    post_spy.assert_called_once_with(path=f'scheduler/{schedule}/{status}')
