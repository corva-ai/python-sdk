from corva.event.event import Event
from corva.event.loader.task import TaskLoader


def test_load(task_event_str):
    """test that sample task event loads without exceptions"""

    event = TaskLoader().load(event=task_event_str)

    assert len(event) == 1
    assert isinstance(event, Event)
