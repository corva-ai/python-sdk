from unittest.mock import patch

import pytest

from corva.app.base import BaseApp
from corva.app.base import ProcessResult
from corva.event.base import BaseEvent
from corva.event.data.base import BaseEventData


class CustomException(Exception):
    """Exception class that supports equality comparisons."""

    def __eq__(self, other):
        return type(self) is type(other) and self.args == other.args


def test_abstractmethods():
    assert getattr(BaseApp, '__abstractmethods__') == frozenset(['event_cls'])
    with pytest.raises(TypeError):
        BaseApp()


def test_pre_process(base_app):
    event = 'myevent'
    pre_res = base_app.pre_process(event=event)
    base_app.event_cls.load.assert_called_once_with(event=event)
    assert pre_res == ProcessResult(event=event, next_process_kwargs={})


def test_process(base_app, patch_base_event):
    base_event = BaseEvent(data=[])
    assert base_app.process(event=base_event) == ProcessResult(event=base_event, next_process_kwargs={})


def test_post_process(base_app, patch_base_event):
    base_event = BaseEvent(data=[])
    assert (
         base_app.post_process(event=base_event, exc=None)
         ==
         ProcessResult(event=base_event, next_process_kwargs={})
    )


def test_on_fail_before_post_process(base_app):
    assert base_app.on_fail_before_post_process(event='myevent') is None


def test_run_correct_params(base_app, patch_base_event):
    event = 'myevent'

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
        base_app.run(
            event=event,
            pre_process_kwargs=pre_process_kwargs,
            process_kwargs=process_kwargs,
            post_process_kwargs=post_process_kwargs
        )
        pre_process.assert_called_once_with(event=event, **pre_process_kwargs)
        process.assert_called_once_with(
            event=pre_process_result.event,
            **{**pre_process_result.next_process_kwargs, **process_kwargs}
        )
        post_process.assert_called_once_with(
            event=process_result.event,
            **{**process_result.next_process_kwargs, **post_process_kwargs}
        )
        on_fail_before_post_process.assert_not_called()


def test_run_exc_in_pre_process(base_app):
    event = 'myevent'

    on_fail_before_post_process_kwargs = {'fail_key_1': 'fail_val_1'}

    with patch.object(base_app, 'pre_process', side_effect=CustomException('')) as pre_process, \
         patch.object(base_app, 'process') as process, \
         patch.object(base_app, 'on_fail_before_post_process') as on_fail_before_post_process, \
         patch.object(base_app, 'post_process') as post_process:
        with pytest.raises(CustomException) as exc:
            base_app.run(
                event=event,
                on_fail_before_post_process_kwargs=on_fail_before_post_process_kwargs
            )
        assert str(exc.value) == ''
        assert pre_process.call_count == 1
        on_fail_before_post_process.assert_called_once_with(event=event, **on_fail_before_post_process_kwargs)
        process.assert_not_called()
        post_process.assert_not_called()


def test_run_exc_in_process(base_app, patch_base_event):
    event = 'myevent'

    on_fail_before_post_process_kwargs = {'fail_key_1': 'fail_val_1'}

    pre_process_result = ProcessResult(event=BaseEvent([]))

    with patch.object(base_app, 'pre_process', return_value=pre_process_result) as pre_process, \
         patch.object(base_app, 'process', side_effect=CustomException('')) as process, \
         patch.object(base_app, 'on_fail_before_post_process') as on_fail_before_post_process, \
         patch.object(base_app, 'post_process') as post_process:
        with pytest.raises(CustomException) as exc:
            base_app.run(
                event=event,
                on_fail_before_post_process_kwargs=on_fail_before_post_process_kwargs
            )
        assert str(exc.value) == ''
        assert pre_process.call_count == 1
        assert process.call_count == 1
        on_fail_before_post_process.assert_called_once_with(
            event=pre_process_result.event, **on_fail_before_post_process_kwargs
        )
        post_process.assert_not_called()


def test_run(base_app, patch_base_event):
    event = 'myevent'

    pre_process_result = ProcessResult(event=BaseEvent([]))
    process_result = ProcessResult(event=BaseEvent([]))

    with patch.object(base_app, 'pre_process', return_value=pre_process_result) as pre_process, \
         patch.object(base_app, 'process', return_value=process_result) as process, \
         patch.object(base_app, 'on_fail_before_post_process') as on_fail_before_post_process, \
         patch.object(base_app, 'post_process') as post_process:
        base_app.run(event=event)
        assert pre_process.call_count == 1
        assert process.call_count == 1
        assert post_process.call_count == 1
        on_fail_before_post_process.assert_not_called()
