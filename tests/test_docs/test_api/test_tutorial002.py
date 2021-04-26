import contextlib
from json import JSONDecodeError

import pytest
from requests_mock import Mocker as RequestsMocker

from corva.models.task import TaskEvent
from docs.src.api import tutorial002


@pytest.mark.parametrize(
    'json,ctx', ([{}, contextlib.nullcontext()], [None, pytest.raises(JSONDecodeError)])
)
def test_tutorial002(json, ctx, app_runner, requests_mock: RequestsMocker):
    event = TaskEvent(asset_id=0, company_id=0)

    mock1 = requests_mock.get('/v2/pads', json=json)
    mock2 = requests_mock.get('/v2/pads?company=1', complete_qs=True)

    with ctx:
        app_runner(tutorial002.task_app, event)

    assert mock1.called_once
    assert mock2.called_once
