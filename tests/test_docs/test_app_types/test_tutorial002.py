from corva.models.stream.stream import StreamDepthEvent, StreamDepthRecord
from docs.src.app_types import tutorial002


def test_tutorial002(app_runner):
    event = StreamDepthEvent(
        asset_id=0, company_id=0, records=[StreamDepthRecord(measured_depth=0)]
    )

    assert app_runner(tutorial002.stream_depth_app, event) == 'Hello, World!'
