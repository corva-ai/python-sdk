from pytest_mock import MockerFixture

from corva.api import Api
from corva.models.task import TaskEvent
from docs.src.api import tutorial005


def test_tutorial005(app_runner, mocker: MockerFixture):
    event = TaskEvent(asset_id=0, company_id=0)

    mock = mocker.patch.object(Api, 'get_dataset')

    app_runner(tutorial005.task_app, event)

    mock.assert_called_once()
