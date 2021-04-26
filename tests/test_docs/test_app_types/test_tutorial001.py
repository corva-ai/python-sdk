from corva.models.stream.stream import StreamTimeEvent, StreamTimeRecord
from docs.src.app_types import tutorial001


def test_tutorial001(app_runner):
    event = StreamTimeEvent(
        asset_id=0, company_id=0, records=[StreamTimeRecord(timestamp=0)]
    )

    assert app_runner(tutorial001.stream_time_app, event) == 'Hello, World!'
