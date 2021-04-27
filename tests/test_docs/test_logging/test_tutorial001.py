import logging

from pytest_mock import MockerFixture

from corva import Logger
from corva.models.task import TaskEvent
from docs.src.logging import tutorial001


def test_tutorial001(app_runner, mocker: MockerFixture, capsys):
    event = TaskEvent(asset_id=0, company_id=0)

    Logger.setLevel('DEBUG')

    # Add handler, as Logger doesnt have one by default
    handler = logging.StreamHandler()
    handler.setLevel('DEBUG')
    mocker.patch.object(Logger, 'handlers', [handler])

    app_runner(tutorial001.task_app, event)

    expected = (
        'Debug message!\nInfo message!\nWarning message!\n'
        'Error message!\nException message!\n'
    )

    captured = capsys.readouterr().err

    assert captured.startswith(expected)
