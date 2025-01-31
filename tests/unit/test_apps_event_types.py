import json
from unittest import mock

import pytest

from corva.configuration import SETTINGS
from corva.handlers import scheduled, stream, task
from corva.models.scheduled.raw import (
    RawScheduledDepthEvent,
    RawScheduledEvent,
    RawScheduledNaturalTimeEvent,
)
from corva.models.scheduled.scheduler_type import SchedulerType
from corva.models.stream.log_type import LogType
from corva.models.stream.raw import (
    RawAppMetadata,
    RawDepthRecord,
    RawMetadata,
    RawStreamDepthEvent,
    RawStreamEvent,
    RawStreamTimeEvent,
    RawTimeRecord,
)
from corva.models.task import RawTaskEvent
from corva.validate_app_init import (
    read_manifest,
    validate_app_type_context,
    validate_event_payload,
)

raw_scheduled_natural_time_event = RawScheduledNaturalTimeEvent(
    asset_id=int(),
    interval=int(),
    schedule=int(),
    app_connection=int(),
    app_stream=int(),
    company=int(),
    scheduler_type=SchedulerType.natural_time,
    schedule_start=int(),
).dict(
    by_alias=True,
    exclude_unset=True,
)

raw_scheduled_depth_event = RawScheduledDepthEvent(
    asset_id=int(),
    depth_milestone=float(),
    schedule=int(),
    app_connection=int(),
    app_stream=int(),
    company=int(),
    scheduler_type=SchedulerType.data_depth_milestone,
    top_depth=0.0,
    bottom_depth=1.0,
    log_identifier='',
).dict(
    by_alias=True,
    exclude_unset=True,
)

stream_time_event = RawStreamTimeEvent(
    records=[
        RawTimeRecord(
            asset_id=0,
            company_id=int(),
            collection=str(),
            timestamp=int(),
        ),
    ],
    metadata=RawMetadata(
        app_stream_id=int(),
        apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=1)},
        log_type=LogType.time,
    ),
).dict()

stream_depth_event = RawStreamDepthEvent(
    records=[
        RawDepthRecord(
            asset_id=int(),
            company_id=int(),
            collection=str(),
            measured_depth=float(),
        )
    ],
    metadata=RawMetadata(
        app_stream_id=int(),
        apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=int())},
        log_type=LogType.depth,
        log_identifier='log_identifier',
    ),
).dict()

task_event = RawTaskEvent(task_id='0', version=2).dict()


@pytest.mark.parametrize(
    'app_decorator,event_payload',
    (
        (task, raw_scheduled_natural_time_event),
        (task, raw_scheduled_depth_event),
        (task, [stream_time_event]),
        (task, [stream_depth_event]),
        (scheduled, task_event),
        (scheduled, [stream_time_event]),
        (scheduled, [stream_depth_event]),
        (stream, task_event),
        (stream, raw_scheduled_natural_time_event),
        (stream, raw_scheduled_depth_event),
    ),
)
def test__lambda_call_with_mismatched_event_type__raise_error(
    app_decorator,
    event_payload,
    context,
):
    @app_decorator
    def lambda_handler(_event, _api):
        ...

    with pytest.raises(RuntimeError):
        lambda_handler(
            event_payload,
            context,
        )


@pytest.mark.parametrize(
    'app_decorator,manifested_app_type',
    (
        (task, "stream"),
        (task, "scheduler"),
        (scheduled, "task"),
        (scheduled, "stream"),
        (stream, "task"),
        (stream, "scheduler"),
    ),
)
def test__lambda_with_mismatched_manifested_type__raise_error(
    app_decorator,
    manifested_app_type,
    context,
):
    @app_decorator
    def lambda_handler(_event, _api):
        ...

    mocked_manifest = {"application": {"type": manifested_app_type}}

    with mock.patch(
        "corva.validate_app_init.read_manifest", return_value=mocked_manifest
    ):
        with pytest.raises(RuntimeError):
            lambda_handler(
                {},
                context,
            )


def test__if_manifested_app_type_is_none__payload_based_validation_called(context):
    mocked_manifest = {"application": {"type": None}}

    with mock.patch(
        "corva.validate_app_init.read_manifest", return_value=mocked_manifest
    ):
        with mock.patch(
            "corva.validate_app_init.validate_event_payload",
            wraps=validate_event_payload,
        ) as mocked_validate_event_payload:
            validate_app_type_context(aws_event=task_event, raw_event_type=RawTaskEvent)

    mocked_validate_event_payload.assert_called_once()


def test__validate_app_type_with_wrong_app_type_at_manifest__raise_error(context):
    mocked_manifest = {"application": {"type": "wrong_type"}}

    with mock.patch(
        "corva.validate_app_init.read_manifest", return_value=mocked_manifest
    ):
        with pytest.raises(ValueError, match="'wrong_type' is not a valid AppType"):
            validate_app_type_context(aws_event=None, raw_event_type=RawTaskEvent)


@pytest.mark.parametrize(
    'manifested_app_type, raw_base_event_type',
    (
        ("task", RawTaskEvent),
        ("stream", RawStreamEvent),
        ("scheduler", RawScheduledEvent),
    ),
)
def test__right_manifested_app_type_and_raw_event_type_passed__success(
    manifested_app_type,
    raw_base_event_type,
):
    mocked_manifest = {"application": {"type": manifested_app_type}}

    with mock.patch(
        "corva.validate_app_init.read_manifest", return_value=mocked_manifest
    ):
        validate_app_type_context(aws_event="any", raw_event_type=raw_base_event_type)


def test__read_correct_manifest_file__success(context):
    """Test when manifest.json exists and contains valid JSON."""
    manifest_payload = {"application": {"type": "stream"}}
    manifest_payload_json = json.dumps(manifest_payload)

    with mock.patch("os.path.exists", return_value=True):
        with mock.patch(
            "builtins.open", mock.mock_open(read_data=manifest_payload_json)
        ):
            result = read_manifest()
            assert result == manifest_payload


def test__read_invalid_json_manifest_file__error(context):
    """Test when manifest.json contains invalid JSON."""
    manifest_invalid_json = "{invalid: json}"

    with mock.patch("os.path.exists", return_value=True):
        with mock.patch(
            "builtins.open", mock.mock_open(read_data=manifest_invalid_json)
        ):
            with pytest.raises(json.JSONDecodeError):
                read_manifest()


def test__manifest_file_not_exists__success(context):
    """Test when manifest.json contains invalid JSON."""
    with mock.patch("os.path.exists", return_value=False):
        result = read_manifest()
        assert result is None
