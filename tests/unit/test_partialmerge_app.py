"""Module contains functional requirements for partilmerge app."""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from corva.handlers import partialmerge

ASSET_VALUE_TO_CACHE = str(uuid4())
RERUN_ASSET_VALUE_TO_CACHE = str(uuid4())
RAW_EVENT = {
    "event_type": "partial-well-rerun-merge",
    "data": {
        "partial_well_rerun_id": 123,
        "partition": 95,
        "rerun_partition": 2,
        "rerun_mode": "realtime",
        "start": 1543847760,
        "end": 1543847760,
        "asset_id": 1231234,
        "rerun_asset_id": 2323245,
        "app_stream_id": 9585,
        "rerun_app_stream_id": 4745,
        "version": 1,
        "app_id": 46,
        "app_key": "corva.enrichment-wrapper",
        "app_connection_ids": [2457, 2458],
        "rerun_app_connection_ids": [2459, 2460],
        "source_type": "drilling",
        "log_type": "time",
        "run_until": 1543847760,
    },
}


def test_partialmerge_app_on_unexpected_event_type_raises_validation_exception(context):
    """Partial merge app must raise Pydantic validation exception
    while processing event of unexpected type.
    """

    @partialmerge
    def partialmerge_app(event, api, asset_cache, rerun_asset_cache):

        return event

    raw_event = dict(RAW_EVENT)
    raw_event["event_type"] = "unknown_event_type"
    with pytest.raises(ValidationError) as e:
        partialmerge_app(raw_event, context)
    assert "value is not a valid enumeration" in str(e.value)


def test_partialmerge_app_returns_expected_cache_values(context):
    """Partial merge app must provide functional cache objects
    for asset and rerun asset.
    """

    @partialmerge
    def partialmerge_app(event, api, asset_cache, rerun_asset_cache):
        asset_cache_key = "asset_cache_key"
        rerun_asset_cache_key = "rerun_asset_cache_key"
        asset_cache.set(key=asset_cache_key, value=ASSET_VALUE_TO_CACHE)
        rerun_asset_cache.set(
            key=rerun_asset_cache_key, value=RERUN_ASSET_VALUE_TO_CACHE
        )
        return asset_cache.get(key=asset_cache_key), rerun_asset_cache.get(
            key=rerun_asset_cache_key
        )

    asset_cached_value, rerun_asset_cached_value = partialmerge_app(RAW_EVENT, context)[
        0
    ]
    assert (asset_cached_value, rerun_asset_cached_value) == (
        ASSET_VALUE_TO_CACHE,
        RERUN_ASSET_VALUE_TO_CACHE,
    ), "Values retrieved from cache should match values stored to cache."
    assert asset_cached_value != rerun_asset_cached_value
