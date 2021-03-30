import contextlib

import pydantic
import pytest
from pytest_mock import MockerFixture

from corva.application import Corva
from corva.models.scheduled import ScheduledEvent
from corva.models.stream import (
    StreamDepthEvent,
    StreamDepthRecord,
    StreamTimeEvent,
    StreamTimeRecord,
)
from corva.models.task import TaskEvent


def test_task_app_runner(app_runner):
    """Should not raise."""

    def lambda_handler(event, context):
        return Corva(context).task(fn=lambda event, api: 'Task app result', event=event)

    event = TaskEvent(asset_id=int(), company_id=int())

    assert app_runner(lambda_handler, event) == 'Task app result'


@pytest.mark.parametrize(
    'asset_id',
    (
        1,
        2,
    ),
)
def test_scheduled_app_runner_sets_correct_asset_id(
    asset_id, app_runner, mocker: MockerFixture
):
    def lambda_handler(event, context):
        return Corva(context).scheduled(fn=lambda event, api, cache: None, event=event)

    # override scheduled_runner to return event from context
    mocker.patch(
        'corva.application.scheduled_runner', lambda fn, context: context.event
    )

    event = ScheduledEvent(asset_id=asset_id, time_from=int(), time_to=int())

    raw_event = app_runner(lambda_handler, event)

    assert raw_event.asset_id == event.asset_id


def test_scheduled_app_runner(app_runner):
    """Should not raise."""

    def lambda_handler(event, context):
        return Corva(context).scheduled(
            fn=lambda event, api, cache: 'Scheduled app result', event=event
        )

    event = ScheduledEvent(asset_id=int(), time_from=int(), time_to=int())

    assert app_runner(lambda_handler, event) == 'Scheduled app result'


@pytest.mark.parametrize(
    'event',
    (
        StreamTimeEvent(
            asset_id=int(),
            company_id=int(),
            records=[StreamTimeRecord(timestamp=int())],
        ),
        StreamDepthEvent(
            asset_id=int(),
            company_id=int(),
            records=[StreamDepthRecord(measured_depth=float())],
        ),
    ),
)
def test_stream_app_runner(event, app_runner):
    """Should not raise."""

    def lambda_handler(event, context):
        return Corva(context).stream(
            fn=lambda event, api, cache: 'Stream app result', event=event
        )

    assert app_runner(lambda_handler, event) == 'Stream app result'


@pytest.mark.parametrize(
    'event',
    (
        StreamTimeEvent(
            asset_id=1,
            company_id=int(),
            records=[StreamTimeRecord(timestamp=int())],
        ),
        StreamDepthEvent(
            asset_id=1,
            company_id=int(),
            records=[StreamDepthRecord(measured_depth=float())],
        ),
        StreamTimeEvent(
            asset_id=2,
            company_id=int(),
            records=[StreamTimeRecord(timestamp=int())],
        ),
        StreamDepthEvent(
            asset_id=2,
            company_id=int(),
            records=[StreamDepthRecord(measured_depth=float())],
        ),
    ),
)
def test_stream_app_runner_sets_correct_asset_id(
    event, app_runner, mocker: MockerFixture
):
    def lambda_handler(event, context):
        return Corva(context).stream(fn=lambda event, api, cache: None, event=event)

    # override stream_runner to return event from context
    mocker.patch('corva.application.stream_runner', lambda fn, context: context.event)

    raw_event = app_runner(lambda_handler, event)

    assert raw_event.asset_id == event.asset_id


@pytest.mark.parametrize(
    'event,exc_ctx',
    (
        [
            StreamTimeEvent(
                asset_id=int(),
                company_id=int(),
                records=[],
            ),
            pytest.raises(
                pydantic.ValidationError,
                match=r'1 validation error [\s\S]* At least one record should be provided\.',
            ),
        ],
        [
            StreamTimeEvent(
                asset_id=int(),
                company_id=int(),
                records=[StreamTimeRecord(timestamp=int())],
            ),
            contextlib.nullcontext(),
        ],
    ),
    ids=('raises', 'not raises'),
)
def test_stream_app_runner_raises_for_empty_records(event, exc_ctx, app_runner):
    def lambda_handler(event, context):
        return Corva(context).stream(fn=lambda event, api, cache: None, event=event)

    with exc_ctx:
        app_runner(lambda_handler, event)
