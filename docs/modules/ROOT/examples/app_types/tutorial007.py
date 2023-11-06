from corva import (  # <1>
    Api,
    Cache,
    PartialRerunMergeEvent,
    StreamTimeEvent,
    partial_rerun_merge,
    stream,
)


@stream
def stream_app(
        event: StreamTimeEvent,
        api: Api,
        cache: Cache
):
    return "Handling stream event..."


@partial_rerun_merge  # <3>
def partial_rerun_app(
        event: PartialRerunMergeEvent,
        api: Api,
        asset_cache: Cache,
        rerun_asset_cache: Cache,
):  # <2>
    return "Hello, World!"  # <4>
