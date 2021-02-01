from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

from corva.application import Corva


def scheduled_app(event, api, state):
    api.session.request = MagicMock()
    api.post = Mock(wraps=api.post)  # spy on api.post
    return api


def test_set_completed_status():
    event = (
        '[[{"cron_string": "", "environment": "", "app": 0, "app_key": "", "app_connection_id": 0, "app_stream_id": 0, '
        '"source_type": "", "company": 0, "provider": "", "schedule": 0, "interval": 0, '
        '"schedule_start": "1970-01-01T00:00:00", "schedule_end": "1970-01-01T00:00:00", "asset_id": 0, '
        '"asset_name": "", "asset_type": "", "timezone": "", "log_type": ""}]]'
    )
    context = SimpleNamespace(client_context=None)

    app = Corva(context)

    results = app.scheduled(scheduled_app, event)

    results[0].post.assert_called_once_with(path='scheduler/0/completed')
