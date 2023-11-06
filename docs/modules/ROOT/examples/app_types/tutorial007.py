from corva import Api, Cache, PartialRerunMergeEvent, partial_rerun_merge, stream, StreamTimeRecord  # <1>


@stream
def stream_app(
        event: StreamTimeRecord,
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
