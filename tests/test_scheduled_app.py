from unittest.mock import Mock

import pytest

from corva.application import Corva


def test_set_completed_status(corva_context):
    def scheduled_app(event, api, state):
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

    corva = Corva(corva_context)

    results = corva.scheduled(scheduled_app, event)

    results[0].post.assert_called_once_with(path='scheduler/0/completed')


@pytest.mark.parametrize('attr', ('schedule_start', 'schedule_end'))
@pytest.mark.parametrize(
    'value,expected',
    (
        [253402300799, 253402300799],  # 31 December 9999 23:59:59
        [253402300800, 253402300],  # 1 January 10000 00:00:00
    ),
    ids=('no cast performed', 'casted from ms to sec'),
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
