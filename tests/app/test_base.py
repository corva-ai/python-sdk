import pytest
from pytest_mock import MockerFixture

from corva.app.base import BaseApp
from corva.event import Event
from corva.models.base import CorvaBaseModel
from tests.conftest import ComparableException


@pytest.fixture(scope='function')
def base_app(mocker: MockerFixture, api, settings):
    # as BaseApp is an abstract class, we cannot initialize it without overriding all abstract methods,
    # so in order to initialize and test the class we patch __abstractmethods__
    mocker.patch.object(BaseApp, '__abstractmethods__', set())

    return BaseApp(app_key=settings.APP_KEY, cache_url=settings.CACHE_URL, api=api)


def test_run_exc_in_event_loader_load(mocker: MockerFixture, base_app):
    loader_mock = mocker.patch.object(BaseApp, 'event_loader')
    loader_mock.load.side_effect = Exception
    logger_spy = mocker.spy(base_app, 'logger')

    with pytest.raises(Exception):
        base_app.run(event='')

    logger_spy.error.assert_called_once_with('Could not prepare events for run.')


def test_run_exc_in__group_event(mocker: MockerFixture, base_app):
    mocker.patch.object(BaseApp, 'event_loader')
    mocker.patch.object(base_app, '_group_event', side_effect=Exception)
    logger_spy = mocker.spy(base_app, 'logger')

    with pytest.raises(Exception):
        base_app.run(event='')

    logger_spy.error.assert_called_once_with('Could not prepare events for run.')


def test_run_runs_for_each_event(mocker: MockerFixture, base_app):
    event1 = Event([CorvaBaseModel(a=1)])
    event2 = Event([CorvaBaseModel(a=2)])

    mocker.patch.object(BaseApp, 'event_loader')
    mocker.patch.object(base_app, '_group_event', return_value=[event1, event2])
    run_mock = mocker.patch.object(base_app, '_run')

    base_app.run(event='')

    assert run_mock.call_count == 2
    run_mock.assert_has_calls([mocker.call(event=event1), mocker.call(event=event2)])


def test__group_event(mocker: MockerFixture, base_app):
    event = Event(
        [CorvaBaseModel(app_connection_id=1),
         CorvaBaseModel(app_connection_id=1),
         CorvaBaseModel(app_connection_id=2)]
    )
    expected = [
        [event[0], event[1]],
        [event[2]]
    ]

    mocker.patch.object(BaseApp, 'group_by_field', new='app_connection_id')

    events = base_app._group_event(event=event)

    assert events == expected


def test__run_exc_in_get_context(mocker: MockerFixture, base_app):
    mocker.patch.object(base_app, 'get_context', side_effect=Exception)
    logger_spy = mocker.spy(base_app, 'logger')

    with pytest.raises(Exception):
        base_app._run(event=Event([]))

    logger_spy.error.assert_called_once_with('Could not get context.')


def test__run_exc_in_pre_process(mocker: MockerFixture, base_app):
    context = 'context'

    mocker.patch.object(base_app, 'get_context', return_value=context)
    mocker.patch.object(base_app, 'pre_process', side_effect=ComparableException)
    logger_spy = mocker.spy(base_app, 'logger')
    on_fail_spy = mocker.spy(base_app, 'on_fail')

    with pytest.raises(ComparableException):
        base_app._run(event=Event([]))

    logger_spy.error.assert_called_once_with('An error occurred in process pipeline.')
    on_fail_spy.assert_called_once_with(context=context, exception=ComparableException())


def test__run_exc_in_process(mocker: MockerFixture, base_app):
    """Tests BaseApp._run function in case of exception in BaseApp.process"""

    context = 'context'

    mocker.patch.object(base_app, 'get_context', return_value=context)
    pre_spy = mocker.spy(base_app, 'pre_process')
    mocker.patch.object(base_app, 'process', side_effect=ComparableException)
    logger_spy = mocker.spy(base_app, 'logger')
    on_fail_spy = mocker.spy(base_app, 'on_fail')

    with pytest.raises(ComparableException):
        base_app._run(event=Event([]))

    pre_spy.assert_called_once_with(context=context)
    logger_spy.error.assert_called_once_with('An error occurred in process pipeline.')
    on_fail_spy.assert_called_once_with(context=context, exception=ComparableException())


def test__run_exc_in_post_process(mocker: MockerFixture, base_app):
    """Tests BaseApp._run function in case of exception in BaseApp.post_process"""

    context = 'context'

    mocker.patch.object(base_app, 'get_context', return_value=context)
    pre_spy = mocker.spy(base_app, 'pre_process')
    process_spy = mocker.spy(base_app, 'process')
    mocker.patch.object(base_app, 'post_process', side_effect=ComparableException)
    logger_spy = mocker.spy(base_app, 'logger')
    on_fail_spy = mocker.spy(base_app, 'on_fail')

    with pytest.raises(ComparableException):
        base_app._run(event=Event([]))

    pre_spy.assert_called_once_with(context=context)
    process_spy.assert_called_once_with(context=context)
    logger_spy.error.assert_called_once_with('An error occurred in process pipeline.')
    on_fail_spy.assert_called_once_with(context=context, exception=ComparableException())


def test__run(mocker: MockerFixture, base_app):
    context = 'context'
    event = Event([])

    get_context_mock = mocker.patch.object(base_app, 'get_context', return_value=context)
    pre_spy = mocker.spy(base_app, 'pre_process')
    process_spy = mocker.spy(base_app, 'process')
    post_spy = mocker.spy(base_app, 'post_process')
    on_fail_spy = mocker.spy(base_app, 'on_fail')

    base_app._run(event=event)

    get_context_mock.assert_called_once_with(event=event)
    pre_spy.assert_called_once_with(context=context)
    process_spy.assert_called_once_with(context=context)
    post_spy.assert_called_once_with(context=context)
    on_fail_spy.assert_not_called()
