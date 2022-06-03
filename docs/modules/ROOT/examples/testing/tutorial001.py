from corva import Api, Cache, StreamTimeEvent, StreamTimeRecord, stream


@stream
def stream_app(event: StreamTimeEvent, api: Api, cache: Cache):  # <.>
    return 'Hello, World!'


def test_stream_time_app(app_runner):  # <.>
    event = StreamTimeEvent(  # <.>
        asset_id=0, company_id=0, records=[StreamTimeRecord(timestamp=0)]
    )

    result = app_runner(stream_app, event=event)  # <.>

    assert result == 'Hello, World!'  # <.>
