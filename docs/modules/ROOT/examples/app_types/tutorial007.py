from corva import Api, Cache, PartialMergeEvent, partialmerge  # <1>


@partialmerge  # <3>
def partialmerge_app(
        event: PartialMergeEvent,
        api: Api,
        asset_cache: Cache,
        rerun_asset_cache: Cache
):  # <2>
    return 'Hello, World!'  # <4>
