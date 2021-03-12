import re
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from corva.application import Corva
from corva.runners.scheduled import scheduled_runner


@pytest.mark.parametrize(
    'status_code',
    (200, 400),
    ids=('request successful', 'request failed - should not raise'),
)
def test_set_completed_status(
    status_code, mocker: MockerFixture, corva_context, requests_mock
):
    def scheduled_app(event, api, state):
        # patch post request, that sets scheduled task as completed
        # looks for url path like /scheduler/123/completed
        requests_mock.post(
            re.compile(r'/scheduler/\d+/completed'), status_code=status_code
        )

        api.post = Mock(wraps=api.post)  # spy on api.post

        return api

    event = [
        [
            {
                "schedule": 0,
                "interval": 0,
                "schedule_start": 0,
                "asset_id": 0,
            }
        ]
    ]

    # corva_patch fixture patches scheduled_runner for user tests.
    # return real scheduled runner instead of patched one.
    mocker.patch('corva.application.scheduled_runner', scheduled_runner)

    corva = Corva(corva_context)

    api = corva.scheduled(scheduled_app, event)[0]

    api.post.assert_called_once_with(path='scheduler/0/completed')


@pytest.mark.parametrize('attr', ('schedule_start', 'schedule_end'))
@pytest.mark.parametrize(
    'value,expected',
    (
        [253402300799, 253402300799],  # 31 December 9999 23:59:59 in sec
        [253402300800, 253402300],  # 1 January 10000 00:00:00 in sec
        [1609459200000, 1609459200],  # 1 January 2021 00:00:00 in ms
        [1609459200, 1609459200],  # 1 January 2021 00:00:00 in sec
    ),
    ids=(
        'no cast performed',
        'casted from ms to sec',
        'casted from ms to sec',
        'no cast performed',
    ),
)
def test_schedule_field_casted_from_ms_if_needed(attr, value, expected, corva_context):
    def app(event, api, state):
        return event

    event = {
        "schedule": 0,
        "interval": 0,
        "schedule_start": 0,
        "asset_id": 0,
        **{attr: value},
    }

    corva = Corva(corva_context)

    event = corva.scheduled(app, event)[0]

    assert getattr(event, attr) == expected
