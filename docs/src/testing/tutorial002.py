from corva import Api, Cache, StreamDepthEvent, StreamDepthRecord, stream


@stream
def stream_app(event: StreamDepthEvent, api: Api, cache: Cache):  # <.>
    return 'Hello, World!'


def test_stream_depth_app(app_runner):  # <.>
    event = StreamDepthEvent(  # <.>
        asset_id=0, company_id=0, records=[StreamDepthRecord(measured_depth=0)]
    )

    result = app_runner(stream_app, event=event)  # <.>

    assert result == 'Hello, World!'  # <.>
