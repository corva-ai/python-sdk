from types import SimpleNamespace
from unittest.mock import call
from unittest.mock import patch

from worker.app.base import BaseApp
from worker.app.base import ProcessResult
from worker.app.scheduled import ScheduledApp
from worker.event.data.base import BaseEventData
from worker.event.scheduled import ScheduledEvent


def test_post_process():
    event = ScheduledEvent(data=[BaseEventData(schedule=1), BaseEventData(schedule=2)])
    with patch.object(BaseApp, 'post_process', return_value=SimpleNamespace(event=event)) as post_process, \
            patch.object(ScheduledApp, 'update_schedule_status') as update_schedule_status:
        app = ScheduledApp()
        post_result = app.post_process(event=event)
        post_process.assert_called_once()
        assert update_schedule_status.call_count == len(event)
        update_schedule_status.assert_has_calls(
            [
                call(schedule=1, status='completed'),
                call(schedule=2, status='completed')
            ]
        )
        assert post_result == ProcessResult(event=event)


def test_update_schedule_status():
    app = ScheduledApp()
    schedule = 1
    status = 'status'
    with patch.object(app, 'api') as api_mock:
        app.update_schedule_status(schedule=schedule, status=status)
        api_mock.post.assert_called_once_with(path=f'scheduler/{schedule}/{status}')
