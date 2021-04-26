from corva.models.scheduled import ScheduledEvent
from docs.src.app_types import tutorial003


def test_tutorial002(app_runner):
    event = ScheduledEvent(asset_id=0, start_time=0, end_time=0)

    assert app_runner(tutorial003.scheduled_app, event) == 'Hello, World!'
