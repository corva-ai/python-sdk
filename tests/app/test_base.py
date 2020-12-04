import pytest
from pytest_mock import MockerFixture

from corva.app.base import BaseApp
from corva.event.data.base import BaseEventData
from corva.event.event import Event


class CustomException(Exception):
    def __eq__(self, other):
        return type(self) is type(other) and self.args == other.args


def test_abstractmethods():
    with pytest.raises(TypeError):
        BaseApp()

    assert (
         getattr(BaseApp, '__abstractmethods__')
         ==
         frozenset([
             'event_loader',
             'group_by_field',
             'get_context'
         ])
    )


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
    mocker.patch.object(BaseApp, 'event_loader')
    event1 = Event(data=[BaseEventData(a=1)])
    event2 = Event(data=[BaseEventData(a=2)])
    mocker.patch.object(base_app, '_group_event', return_value=[event1, event2])
    run_mock = mocker.patch.object(base_app, '_run'
                                   )
    base_app.run(event='')
    assert run_mock.call_count == 2
    run_mock.assert_has_calls([mocker.call(event=event1), mocker.call(event=event2)])


def test__group_event(mocker: MockerFixture, base_app):
    mocker.patch.object(BaseApp, 'group_by_field', new='app_connection_id')

    event = Event(data=[
        BaseEventData(app_connection_id=1),
        BaseEventData(app_connection_id=1),
        BaseEventData(app_connection_id=2)]
    )
    events = base_app._group_event(event=event)
    expected = [
        [event[0], event[1]],
        [event[2]]
    ]
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
    mocker.patch.object(base_app, 'pre_process', side_effect=CustomException)
    logger_spy = mocker.spy(base_app, 'logger')
    on_fail_spy = mocker.spy(base_app, 'on_fail')

    with pytest.raises(CustomException):
        base_app._run(event=Event([]))
    logger_spy.error.assert_called_once_with('An error occurred in process pipeline.')
    on_fail_spy.assert_called_once_with(context=context, exception=CustomException())


def test__run_exc_in_process(mocker: MockerFixture, base_app):
    context = 'context'

    mocker.patch.object(base_app, 'get_context', return_value=context)
    pre_spy = mocker.spy(base_app, 'pre_process')
    mocker.patch.object(base_app, 'process', side_effect=CustomException)
    logger_spy = mocker.spy(base_app, 'logger')
    on_fail_spy = mocker.spy(base_app, 'on_fail')

    with pytest.raises(CustomException):
        base_app._run(event=Event([]))
    pre_spy.assert_called_once_with(context=context)
    logger_spy.error.assert_called_once_with('An error occurred in process pipeline.')
    on_fail_spy.assert_called_once_with(context=context, exception=CustomException())


def test__run_exc_in_post_process(mocker: MockerFixture, base_app):
    context = 'context'

    mocker.patch.object(base_app, 'get_context', return_value=context)
    pre_spy = mocker.spy(base_app, 'pre_process')
    process_spy = mocker.spy(base_app, 'process')
    mocker.patch.object(base_app, 'post_process', side_effect=CustomException)
    logger_spy = mocker.spy(base_app, 'logger')
    on_fail_spy = mocker.spy(base_app, 'on_fail')

    with pytest.raises(CustomException):
        base_app._run(event=Event([]))
    pre_spy.assert_called_once_with(context=context)
    process_spy.assert_called_once_with(context=context)
    logger_spy.error.assert_called_once_with('An error occurred in process pipeline.')
    on_fail_spy.assert_called_once_with(context=context, exception=CustomException())


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
