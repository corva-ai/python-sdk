from types import SimpleNamespace
from unittest.mock import Mock, patch

from corva.application import Corva


def scheduled_app(event, api, state):
    patch('requests.request').start()

    api.post = Mock(wraps=api.post)  # spy on api.post

    return api


def test_set_completed_status():
    event = [
        [
            {
                "app_connection": 0,
                "app_stream": 0,
                "schedule": 0,
                "interval": 0,
                "schedule_start": 0,
                "schedule_end": 0,
                "asset_id": 0,
            }
        ]
    ]

    corva = Corva(SimpleNamespace(client_context=None))

    results = corva.scheduled(scheduled_app, event)

    results[0].post.assert_called_once_with(path='scheduler/0/completed')
