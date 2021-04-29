from corva import Api, Cache, StreamTimeEvent, stream  # <1>


@stream  # <3>
def stream_time_app(event: StreamTimeEvent, api: Api, cache: Cache):  # <2>
    return "Hello, World!"  # <4>
