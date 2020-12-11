import pytest

from corva.loader.task import TaskLoader


@pytest.fixture(scope='session')
def task_event_str() -> str:
    return '{"task_id": "dc5c16e4-c0e3-43fc-85b2-83b08b9da7d0", "version": 2}'


def test_load(task_event_str):
    """test that sample task event loads without exceptions"""

    event = TaskLoader().load(event=task_event_str)

    assert len(event) == 1
