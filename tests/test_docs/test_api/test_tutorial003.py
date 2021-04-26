from requests_mock import Mocker as RequestsMocker

from corva.models.task import TaskEvent
from docs.src.api import tutorial003


def test_tutorial003(app_runner, requests_mock: RequestsMocker):
    event = TaskEvent(asset_id=0, company_id=0)

    post_mock = requests_mock.post('/v2/pads')
    delete_mock = requests_mock.delete('/v2/pads/123')
    put_mock = requests_mock.put('/api/v1/data/provider/dataset/')
    patch_mock = requests_mock.patch('/v2/pads/123')

    app_runner(tutorial003.task_app, event)

    assert post_mock.last_request._request.body.decode() == '{"key": "val"}'
    assert delete_mock.called_once
    assert put_mock.last_request._request.body.decode() == '{"key": "val"}'
    assert patch_mock.last_request._request.body.decode() == '{"key": "val"}'
