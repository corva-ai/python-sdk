from corva import Api, Cache, PartialRerunMergeEvent, partial_rerun_merge  # <1>


@partial_rerun_merge  # <3>
def partialmerge_app(
    event: PartialRerunMergeEvent, api: Api, asset_cache: Cache, rerun_asset_cache: Cache
):  # <2>
    return "Hello, World!"  # <4>
