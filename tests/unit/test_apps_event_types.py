import json
from unittest import mock

import pytest

from corva.handlers import scheduled, stream, task
from corva.models.scheduled.raw import (
    RawScheduledDepthEvent,
    RawScheduledNaturalTimeEvent,
)
from corva.models.scheduled.scheduler_type import SchedulerType
from corva.models.stream.stream import (
    StreamDepthEvent,
    StreamDepthRecord,
    StreamTimeEvent,
    StreamTimeRecord,
)
from corva.models.task import RawTaskEvent
from corva.validate_app_init import read_manifest, validate_app_type_context

raw_scheduled_natural_time_event = RawScheduledNaturalTimeEvent(
    asset_id=1,
    interval=1,
    schedule=1,
    app_connection=1,
    app_stream=1,
    company=1,
    scheduler_type=SchedulerType.natural_time,
    schedule_start=1,
)

raw_scheduled_depth_event = RawScheduledDepthEvent(
    asset_id=1,
    depth_milestone=1.0,
    schedule=1,
    app_connection=1,
    app_stream=1,
    company=1,
    scheduler_type=SchedulerType.data_depth_milestone,
    top_depth=0.0,
    bottom_depth=1.0,
    log_identifier='',
)

stream_time_event = StreamTimeEvent(
    asset_id=0, company_id=0, records=[StreamTimeRecord(timestamp=0)]
)

stream_depth_event = StreamDepthEvent(
    asset_id=0, company_id=0, records=[StreamDepthRecord(measured_depth=0)]
)

task_event = RawTaskEvent(task_id='0', version=2)


@pytest.mark.parametrize(
    'app_decorator,event_payload',
    (
        (task, raw_scheduled_natural_time_event),
        (task, raw_scheduled_depth_event),
        (task, stream_time_event),
        (task, stream_depth_event),
        (scheduled, task_event),
        (scheduled, stream_time_event),
        (scheduled, stream_depth_event),
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
    def lambda_handler(_event, _api):
        ...

    particular_app = app_decorator(lambda_handler)

    with pytest.raises(RuntimeError):
        particular_app(
            event_payload.dict(),
            context,
        )


@pytest.mark.parametrize(
    'app_decorator,manifested_app_type',
    (
        (task, "stream"),
        (task, "scheduled"),
        (scheduled, "task"),
        (scheduled, "stream"),
        (stream, "task"),
        (stream, "scheduled"),
    ),
)
def test__lambda_with_mismatched_manifested_type__raise_error(
    app_decorator,
    manifested_app_type,
    context,
):
    def lambda_handler(_event, _api):
        ...

    particular_app = app_decorator(lambda_handler)

    mocked_manifest = {"application": {"type": manifested_app_type}}

    with mock.patch(
        "corva.validate_app_init.read_manifest", return_value=mocked_manifest
    ):
        with pytest.raises(RuntimeError):
            particular_app(
                {},
                context,
            )


def test__validate_app_type_with_missed_app_type_at_manifested__does_not_fails(context):
    mocked_manifest = {"application": {"type": None}}

    with mock.patch(
        "corva.validate_app_init.read_manifest", return_value=mocked_manifest
    ):
        validate_app_type_context(aws_event="mocked", app_decorator_type="mocked")


def test__read_correct_manifest_file__success(context):
    """Test when manifest.json exists and contains valid JSON."""
    manifest_payload = {"application": {"type": "stream"}}
    manifest_payload_json = json.dumps(manifest_payload)

    with (
        mock.patch("os.path.exists", return_value=True),
        mock.patch("builtins.open", mock.mock_open(read_data=manifest_payload_json)),
    ):
        result = read_manifest()
        assert result == manifest_payload


def test__read_invalid_json_manifest_file__error(context):
    """Test when manifest.json contains invalid JSON."""
    manifest_invalid_json = "{invalid: json}"

    with (
        mock.patch("os.path.exists", return_value=True),
        mock.patch("builtins.open", mock.mock_open(read_data=manifest_invalid_json)),
    ):
        with pytest.raises(json.JSONDecodeError):
            read_manifest()


def test__manifest_file_not_exists__success(context):
    """Test when manifest.json contains invalid JSON."""
    with mock.patch("os.path.exists", return_value=False):
        result = read_manifest()
        assert result is None
