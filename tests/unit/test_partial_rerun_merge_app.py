"""Module contains functional requirements for partial rerun merge event handler."""
from copy import deepcopy
from uuid import uuid4

import pytest
from pydantic import ValidationError

import corva

ASSET_VALUE_TO_CACHE = str(uuid4())
RERUN_ASSET_VALUE_TO_CACHE = str(uuid4())
RAW_EVENT = {
    "event_type": "partial-well-rerun-merge",
    "data": {
        "partial_well_rerun_id": 123,
        "rerun_mode": "realtime",
        "start": 1543847760,
        "end": 1543847760,
        "asset_id": 1231234,
        "rerun_asset_id": 2323245,
        "app_stream_id": 9585,
        "rerun_app_stream_id": 4745,
        "version": 2,  # That is the partial re-run version, not schema version.
        "app_id": 46,
        "app_key": "corva.enrichment-wrapper",
        "app_connection_id": 2457,
        "rerun_app_connection_id": 2459,
        "source_type": "drilling",
        "log_type": "time",
        "run_until": 1543847760,
    },
}


def test_merge_event_handler_called_from_stream_app_on_unexpected_event_type_raises_exc(
    context,
):
    """Partial merge app must raise Pydantic validation exception
    while processing event of unexpected type.
    """

    @corva.stream
    def stream_app(event, api, cache):
        pytest.fail("Stream app was unexpectedly called!")

    @corva.partial_rerun_merge
    def partial_rerun_merge_app(event, api, asset_cache, rerun_asset_cache):
        pytest.fail("Partial rerun merge app was unexpectedly executed!")

    raw_event = dict(RAW_EVENT)
    raw_event["event_type"] = "unknown_event_type"

    with pytest.raises(RuntimeError) as e:
        stream_app(raw_event, context)
    assert 'Application with type "stream" was invoked with "unknown" event type' in str(e.value)


def test_merge_event_handler_called_from_stream_app_returns_expected_cache_values(
    context,
):
    """Partial merge app must provide functional(working) cache objects
    for asset and rerun asset.
    """

    @corva.stream
    def stream_app(event, api, cache):
        pytest.fail("Stream app was unexpectedly called!")

    @corva.partial_rerun_merge
    def partial_rerun_merge_app(event, api, asset_cache, rerun_asset_cache):
        asset_cache_key = "asset_cache_key"
        rerun_asset_cache_key = "rerun_asset_cache_key"
        asset_cache.set(key=asset_cache_key, value=ASSET_VALUE_TO_CACHE)
        rerun_asset_cache.set(
            key=rerun_asset_cache_key, value=RERUN_ASSET_VALUE_TO_CACHE
        )
        return asset_cache.get(key=asset_cache_key), rerun_asset_cache.get(
            key=rerun_asset_cache_key
        )

    asset_cached_value, rerun_asset_cached_value = stream_app(RAW_EVENT, context)[0]
    assert (asset_cached_value, rerun_asset_cached_value) == (
        ASSET_VALUE_TO_CACHE,
        RERUN_ASSET_VALUE_TO_CACHE,
    ), "Values retrieved from cache should match values stored to cache."
    assert asset_cached_value != rerun_asset_cached_value


def test_merge_event_handler_called_from_stream_app_calls_needed_handler(context):
    """When calling stream event with merge event handler defined,
    partial merge handler should be called only.
    """

    expected_response = str(uuid4())

    @corva.stream
    def stream_app(event, api, cache):
        pytest.fail("Stream app was unexpectedly called!")

    @corva.partial_rerun_merge
    def partial_rerun_merge_app(event, api, asset_cache, rerun_asset_cache):
        return expected_response

    partial_merge_app_response = stream_app(RAW_EVENT, context)[0]
    assert partial_merge_app_response == expected_response


def test_merge_event_handler_called_from_scheduled_app_calls_needed_handler(context):
    """When calling scheduled event with merge event handler defined,
    partial merge handler should be called only.
    """

    expected_response = str(uuid4())

    @corva.scheduled
    def scheduled_app(event, api, cache):
        pytest.fail("Scheduled app was unexpectedly called!")

    @corva.partial_rerun_merge
    def partial_rerun_merge_app(event, api, asset_cache, rerun_asset_cache):
        return expected_response

    partial_merge_app_response = scheduled_app(RAW_EVENT, context)[0]
    assert partial_merge_app_response == expected_response


def test_event_parsing_not_failing_on_missing_field(context):
    """
    When field is missing we're still able to parse the event and invoke correct
    function
    """

    @corva.scheduled(merge_events=True)
    def scheduled_app(event, api, cache):
        pytest.fail("Scheduled app was unexpectedly called!")

    @corva.partial_rerun_merge
    def partial_rerun_merge_app(event, api, asset_cache, rerun_asset_cache):
        return True

    event_with_missing_parameter: dict = deepcopy(RAW_EVENT)
    event_with_missing_parameter["data"]["end"] = None
    partial_merge_app_response = scheduled_app(event_with_missing_parameter, context)[0]
    assert partial_merge_app_response is True


def test_merge_events_does_not_fail_for_partial_rerun_merge_events(context, mocker):
    """
    partial rerun merge events shouldn't fail to be processed when used in combination
    with merge_events parameter
    """

    @corva.scheduled(merge_events=True)
    def scheduled_app(event, api, cache):
        pytest.fail("Scheduled app was unexpectedly called!")

    @corva.partial_rerun_merge
    def partial_rerun_merge_app(event, api, asset_cache, rerun_asset_cache):
        return True

    partial_merge_app_response = scheduled_app(RAW_EVENT, context)[0]
    assert partial_merge_app_response is True


def test_partial_rerun_decorator_can_be_called_directly(context):
    corva.partial_rerun_merge()
