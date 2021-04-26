from requests_mock import Mocker as RequestsMocker

from corva.models.task import TaskEvent
from corva.testing import TestClient
from docs.src.api import tutorial004


def test_tutorial004(app_runner, requests_mock: RequestsMocker):
    event = TaskEvent(asset_id=0, company_id=0)

    expected_headers = {
        'header': 'header-value',
        **TestClient._api.default_headers,
    }

    mock = requests_mock.get('/v2/pads')

    app_runner(tutorial004.task_app, event)

    assert mock.call_count == 2
    for header, header_value in expected_headers.items():
        assert mock.request_history[0]._request.headers[header] == header_value
    assert mock.request_history[1].timeout == 5
