from unittest.mock import Mock, MagicMock

from corva.application import Corva

app = Corva()

EVENT = '[[{"cron_string": "", "environment": "", "app": 0, "app_key": "", "app_connection_id": 0, "app_stream_id": 0, "source_type": "", "company": 0, "provider": "", "schedule": 0, "interval": 0, "schedule_start": "1970-01-01T00:00:00", "schedule_end": "1970-01-01T00:00:00", "asset_id": 0, "asset_name": "", "asset_type": "", "timezone": "", "log_type": ""}]]'


def scheduled_app(event, api, state):
    api.session.request = MagicMock()
    api.post = Mock(wraps=api.post)
    return api


def test_run():
    """Test that both usages of decorator run successfully"""

    app.scheduled()(scheduled_app)(EVENT)
    app.scheduled(scheduled_app)(EVENT)


def test_set_completed_status():
    result, = app.scheduled(scheduled_app)(EVENT)

    result.post.assert_called_once_with(path='scheduler/0/completed')
