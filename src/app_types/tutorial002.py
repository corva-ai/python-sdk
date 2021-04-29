from corva import Api, Cache, StreamDepthEvent, stream  # <1>


@stream  # <3>
def stream_depth_app(event: StreamDepthEvent, api: Api, cache: Cache):  # <2>
    return 'Hello, World!'  # <4>
