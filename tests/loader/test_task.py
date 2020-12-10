import pytest

from corva.event import Event
from corva.loader.task import TaskLoader
from tests.conftest import DATA_PATH


@pytest.fixture(scope='session')
def task_event_str() -> str:
    with open(DATA_PATH / 'task_event.json') as task_event:
        return task_event.read()


def test_load(task_event_str):
    """test that sample task event loads without exceptions"""

    event = TaskLoader().load(event=task_event_str)

    assert len(event) == 1
    assert isinstance(event, Event)
