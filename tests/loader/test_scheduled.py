import pytest

from corva.loader.scheduled import ScheduledLoader
from tests.conftest import DATA_PATH


@pytest.fixture(scope='module')
def scheduled_event_str() -> str:
    with open(DATA_PATH / 'scheduled_event.json') as scheduled_event:
        return scheduled_event.read()


def test_load(scheduled_event_str):
    """test that sample scheduled event loaded without exceptions"""

    event = ScheduledLoader().load(event=scheduled_event_str)

    assert len(event) == 3
