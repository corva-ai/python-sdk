from types import SimpleNamespace
from unittest.mock import call
from unittest.mock import patch

from corva.app.base import BaseApp
from corva.app.base import ProcessResult
from corva.event.data.base import BaseEventData
from corva.event.scheduled import ScheduledEvent


def test_post_process(scheduled_app):
    state = ''
    event = ScheduledEvent(data=[BaseEventData(schedule=1), BaseEventData(schedule=2)])
    with patch.object(BaseApp, 'post_process', return_value=SimpleNamespace(event=event)) as post_process, \
         patch.object(scheduled_app, 'update_schedule_status') as update_schedule_status:
        post_result = scheduled_app.post_process(event=event, state=state)
        post_process.assert_called_once_with(event=event, state=state)
        assert update_schedule_status.call_count == len(event)
        update_schedule_status.assert_has_calls(
            [
                call(schedule=1, status='completed'),
                call(schedule=2, status='completed')
            ]
        )
        assert post_result == ProcessResult(event=event)


def test_update_schedule_status(scheduled_app):
    schedule = 1
    status = 'status'
    with patch.object(scheduled_app, 'api') as api_mock:
        scheduled_app.update_schedule_status(schedule=schedule, status=status)
        api_mock.post.assert_called_once_with(path=f'scheduler/{schedule}/{status}')
