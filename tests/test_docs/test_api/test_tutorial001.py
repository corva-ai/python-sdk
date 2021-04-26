from requests_mock import Mocker as RequestsMocker

from corva.models.task import TaskEvent
from docs.src.api import tutorial001


def test_tutorial001(app_runner, requests_mock: RequestsMocker):
    event = TaskEvent(asset_id=0, company_id=0)

    mock1 = requests_mock.get('/v2/pads')
    mock2 = requests_mock.get('/api/v1/data/provider/dataset/')
    mock3 = requests_mock.get('https://api.corva.ai/v2/pads')

    app_runner(tutorial001.task_app, event)

    assert mock1.called_once
    assert mock2.called_once
    assert mock3.called_once
