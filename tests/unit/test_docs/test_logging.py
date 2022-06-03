import logging

from pytest_mock import MockerFixture

from corva import Logger, TaskEvent
from corva.models.task import RawTaskEvent
from docs.modules.ROOT.examples.logging import tutorial001, tutorial002


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


def test_tutorial002(context, mocker: MockerFixture, capsys, caplog):
    raw_event = RawTaskEvent(task_id='0', version=2).dict()
    event = TaskEvent(asset_id=0, company_id=int())

    mocker.patch.object(
        RawTaskEvent,
        'get_task_event',
        return_value=event,
    )
    mocker.patch.object(RawTaskEvent, 'update_task_data')
    mocker.patch.object(Logger, 'propagate', True)  # for caplog to work

    tutorial002.task_app(raw_event, context)

    captured = capsys.readouterr()

    assert captured.out.endswith('Info message!\n')
    assert caplog.messages == ['Info message!']
