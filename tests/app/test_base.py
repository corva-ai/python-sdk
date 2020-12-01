from unittest.mock import patch, call

import pytest

from corva.app.base import BaseApp
from corva.app.base import ProcessResult
from corva.event.base import BaseEvent
from corva.event.data.base import BaseEventData
from corva.utils import get_state_key


def test_abstractmethods():
    assert getattr(BaseApp, '__abstractmethods__') == frozenset(['event_cls'])
    with pytest.raises(TypeError):
        BaseApp()


def test__group_event(base_app):
    event = BaseEvent(data=[
        BaseEventData(app_connection_id=1),
        BaseEventData(app_connection_id=1),
        BaseEventData(app_connection_id=2)]
    )
    events = base_app._group_event(event=event)
    expected = [
        [BaseEventData(app_connection_id=1), BaseEventData(app_connection_id=1)],
        [BaseEventData(app_connection_id=2)]
    ]
    assert events == expected


def test__get_states(base_app):
    event1 = BaseEvent(data=[
        BaseEventData(asset_id=1, app_stream_id=1, app_connection_id=1)
    ])
    event2 = BaseEvent(data=[
        BaseEventData(asset_id=2, app_stream_id=2, app_connection_id=2)
    ])
    events = [event1, event2]

    states = base_app._get_states(events=events)
    for state, event in zip(states, events):
        expected_default_name = get_state_key(
            asset_id=event[0].asset_id,
            app_stream_id=event[0].app_stream_id,
            app_connection_id=event[0].app_connection_id,
            app_key=base_app.app_key
        )
        assert state.redis.default_name == expected_default_name


def test_run_load_params(base_app):
    event = 'event'
    for load_kwargs in [{'key1': 'val1'}, None]:
        with patch.object(base_app.event_cls, 'load', side_effect=Exception) as load_mock, \
             pytest.raises(Exception):
            base_app.run(event=event, load_kwargs=load_kwargs)
        load_kwargs = load_kwargs or {}
        load_mock.assert_called_once_with(event=event, **load_kwargs)


def test_run_exception(base_app):
    for object_, func_name in [
        (base_app.event_cls, 'load'),
        (base_app, '_group_event'),
        (base_app, '_get_states')
    ]:
        with patch.object(object_, func_name, side_effect=Exception), \
             patch.object(base_app, 'logger') as logger_mock, \
             pytest.raises(Exception):
            base_app.run(event='')
        logger_mock.error.assert_called_once_with('Could not prepare events and states for run.')


def test_run(base_app):
    with patch.object(base_app, '_group_event', return_value=[1, 2]), \
         patch.object(base_app, '_get_states', return_value=[3, 4]), \
         patch.object(base_app, '_run') as _run:
        base_app.run('')
        assert _run.call_count == 2
        _run.assert_has_calls([
            call(
                event=1,
                state=3,
                pre_process_kwargs=None,
                process_kwargs=None,
                post_process_kwargs=None,
                on_fail_before_post_process_kwargs=None
            ),
            call(
                event=2,
                state=4,
                pre_process_kwargs=None,
                process_kwargs=None,
                post_process_kwargs=None,
                on_fail_before_post_process_kwargs=None
            )
        ]
        )


def test_pre_process(base_app, redis):
    event = 'myevent'
    pre_res = base_app.pre_process(event=event, state=redis)
    assert pre_res == ProcessResult(event=event, next_process_kwargs={})


def test_process(base_app, patch_base_event, redis):
    base_event = BaseEvent(data=[])
    assert (
         base_app.process(event=base_event, state=redis)
         ==
         ProcessResult(event=base_event, next_process_kwargs={})
    )


def test_post_process(base_app, patch_base_event, redis):
    base_event = BaseEvent(data=[])
    assert (
         base_app.post_process(event=base_event, state=redis)
         ==
         ProcessResult(event=base_event, next_process_kwargs={})
    )


def test_on_fail_before_post_process(base_app, redis):
    assert base_app.on_fail_before_post_process(event='myevent', state=redis) is None


def test__run_correct_params(base_app, patch_base_event):
    event = 'myevent'
    state = ''

    pre_process_kwargs = {'pre_key_1': 'pre_val_1'}
    process_kwargs = {'pro_key_1': 'pro_val_1'}
    post_process_kwargs = {'post_key_1': 'post_val_1'}

    pre_process_result = ProcessResult(
        event=BaseEvent([]),
        next_process_kwargs={
            'pre_next_key_1': 'pre_next_val_1',
            'pro_key_1': 'random'  # should be overridden by process_kwargs
        }
    )
    process_result = ProcessResult(
        event=BaseEvent([BaseEventData()]),
        next_process_kwargs={
            'pro_next_key_1': 'pro_next_val_1',
            'post_key_1': 'random'  # should be overridden by post_process_kwargs
        }
    )

    with patch.object(base_app, 'pre_process', return_value=pre_process_result) as pre_process, \
         patch.object(base_app, 'process', return_value=process_result) as process, \
         patch.object(base_app, 'on_fail_before_post_process') as on_fail_before_post_process, \
         patch.object(base_app, 'post_process') as post_process:
        base_app._run(
            event=event,
            state=state,
            pre_process_kwargs=pre_process_kwargs,
            process_kwargs=process_kwargs,
            post_process_kwargs=post_process_kwargs
        )
        pre_process.assert_called_once_with(event=event, state=state, **pre_process_kwargs)
        process.assert_called_once_with(
            event=pre_process_result.event,
            state=state,
            **{**pre_process_result.next_process_kwargs, **process_kwargs}
        )
        post_process.assert_called_once_with(
            event=process_result.event,
            state=state,
            **{**process_result.next_process_kwargs, **post_process_kwargs}
        )
        on_fail_before_post_process.assert_not_called()


def test__run_exc_in_pre_process(base_app):
    event = 'myevent'
    state = 'state'

    on_fail_before_post_process_kwargs = {'fail_key_1': 'fail_val_1'}

    with patch.object(base_app, 'pre_process', side_effect=Exception) as pre_process, \
         patch.object(base_app, 'process') as process, \
         patch.object(base_app, 'on_fail_before_post_process') as on_fail_before_post_process, \
         patch.object(base_app, 'post_process') as post_process, \
         patch.object(base_app, 'logger') as logger_mock, \
         pytest.raises(Exception) as exc:
        base_app._run(
            event=event,
            state=state,
            on_fail_before_post_process_kwargs=on_fail_before_post_process_kwargs
        )
    logger_mock.error.assert_called_once_with('An error occurred in pre_process or process.')
    assert str(exc.value) == ''
    assert pre_process.call_count == 1
    on_fail_before_post_process.assert_called_once_with(
        event=event, state=state, **on_fail_before_post_process_kwargs
    )
    process.assert_not_called()
    post_process.assert_not_called()


def test__run_exc_in_process(base_app, patch_base_event):
    event = 'myevent'
    state = 'state'

    on_fail_before_post_process_kwargs = {'fail_key_1': 'fail_val_1'}

    pre_process_result = ProcessResult(event=BaseEvent([]))

    with patch.object(base_app, 'pre_process', return_value=pre_process_result) as pre_process, \
         patch.object(base_app, 'process', side_effect=Exception) as process, \
         patch.object(base_app, 'on_fail_before_post_process') as on_fail_before_post_process, \
         patch.object(base_app, 'post_process') as post_process, \
         patch.object(base_app, 'logger') as logger_mock, \
         pytest.raises(Exception) as exc:
        base_app._run(
            event=event,
            state=state,
            on_fail_before_post_process_kwargs=on_fail_before_post_process_kwargs
        )
    logger_mock.error.assert_called_once_with('An error occurred in pre_process or process.')
    assert str(exc.value) == ''
    assert pre_process.call_count == 1
    assert process.call_count == 1
    on_fail_before_post_process.assert_called_once_with(
        event=pre_process_result.event, state=state, **on_fail_before_post_process_kwargs
    )
    post_process.assert_not_called()


def test__run(base_app, patch_base_event):
    event = 'myevent'
    state = 'state'

    pre_process_result = ProcessResult(event=BaseEvent([]))
    process_result = ProcessResult(event=BaseEvent([]))

    with patch.object(base_app, 'pre_process', return_value=pre_process_result) as pre_process, \
         patch.object(base_app, 'process', return_value=process_result) as process, \
         patch.object(base_app, 'on_fail_before_post_process') as on_fail_before_post_process, \
         patch.object(base_app, 'post_process') as post_process:
        base_app._run(event=event, state=state)
        assert pre_process.call_count == 1
        assert process.call_count == 1
        assert post_process.call_count == 1
        on_fail_before_post_process.assert_not_called()


def test__run_except_in_post_process(base_app):
    event = 'event'
    state = 'state'

    with patch.object(base_app, 'pre_process'), \
         patch.object(base_app, 'process'), \
         patch.object(base_app, 'post_process', side_effect=Exception), \
         patch.object(base_app, 'logger') as logger_mock, \
         pytest.raises(Exception):
        base_app._run(event=event, state=state)
    logger_mock.error.assert_called_once_with('An error occurred in post_process.')
