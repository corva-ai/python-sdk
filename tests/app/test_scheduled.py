import pytest
from pytest_mock import MockerFixture

from corva.app.base import BaseApp
from corva.app.scheduled import ScheduledApp
from corva.app.utils.context import ScheduledContext
from corva.event.data.scheduled import ScheduledEventData
from corva.event.event import Event
from corva.event.loader.scheduled import ScheduledLoader
from tests.conftest import ComparableException, APP_KEY, CACHE_URL


@pytest.fixture(scope='function')
def scheduled_app():
    return ScheduledApp(app_key=APP_KEY, cache_url=CACHE_URL)


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
            'event': Event(data=[scheduled_event_data_factory()]),
            'state': redis
        }
        default_params.update(kwargs)

        return ScheduledContext(**default_params)

    return _scheduled_context_factory


@pytest.mark.parametrize(
    'attr_name,expected', (('group_by_field', 'app_connection_id'),)
)
def test_default_values(attr_name, expected):
    assert getattr(ScheduledApp, attr_name) == expected


def test_event_loader(scheduled_app):
    event_loader = scheduled_app.event_loader()

    assert isinstance(event_loader, ScheduledLoader)


def test_get_context(mocker: MockerFixture, scheduled_app):
    mocker.patch('corva.utils.GetStateKey.from_event', return_value='')

    context = scheduled_app.get_context(event=Event(data=[]))

    assert isinstance(context, ScheduledContext)


def test_pre_process_calls_base(mocker: MockerFixture, scheduled_app, scheduled_context_factory):
    context = scheduled_context_factory()

    super_pre_process_mock = mocker.patch.object(BaseApp, 'pre_process')

    scheduled_app.pre_process(context=context)

    super_pre_process_mock.assert_called_once_with(context=context)


def test_process_calls_base(mocker: MockerFixture, scheduled_app, scheduled_context_factory):
    context = scheduled_context_factory()

    super_process_mock = mocker.patch.object(BaseApp, 'process')

    scheduled_app.process(context=context)

    super_process_mock.assert_called_once_with(context=context)


def test_post_process_calls_base(mocker: MockerFixture, scheduled_app, scheduled_context_factory):
    context = scheduled_context_factory()

    super_post_process_mock = mocker.patch.object(BaseApp, 'post_process')
    mocker.patch.object(scheduled_app, 'update_schedule_status')

    scheduled_app.post_process(context=context)

    super_post_process_mock.assert_called_once_with(context=context)


def test_on_fail_calls_base(mocker: MockerFixture, scheduled_app, scheduled_context_factory):
    context = scheduled_context_factory()
    exc = ComparableException('')

    super_on_fail_mock = mocker.patch.object(BaseApp, 'on_fail')

    scheduled_app.on_fail(context=context, exception=exc)

    super_on_fail_mock.assert_called_once_with(context=context, exception=exc)


def test_post_process(
     mocker: MockerFixture, scheduled_app, scheduled_event_data_factory, scheduled_context_factory
):
    event = Event(data=[scheduled_event_data_factory(schedule=1), scheduled_event_data_factory(schedule=2)])
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

    api_mock = mocker.patch.object(scheduled_app, 'api')

    scheduled_app.update_schedule_status(schedule=schedule, status=status)

    api_mock.post.assert_called_once_with(path=f'scheduler/{schedule}/{status}')
