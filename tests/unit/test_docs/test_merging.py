from typing import Dict, List, Union

import pytest

from corva import ScheduledDataTimeEvent, StreamTimeEvent
from corva.configuration import SETTINGS
from corva.models.scheduled.raw import RawScheduledDataTimeEvent
from corva.models.scheduled.scheduler_type import SchedulerType
from corva.models.stream.log_type import LogType
from corva.models.stream.raw import (
    RawAppMetadata,
    RawMetadata,
    RawStreamTimeEvent,
    RawTimeRecord,
)
from docs.modules.ROOT.examples.merging import tutorial001, tutorial002


def test_tutorial001(context):
    """
    merge_events parameter is merging records for "stream" apps

    When 3 events with 2 records each are sent and @stream decorator
    has optional "merge_events" param set to True - we're supposed
    to merge incoming events into one event with all records(6 in our
    case) combined
    """

    event = []
    timestamp = 1
    # generate 3 events with 2 records each
    for _ in range(3):
        event.extend(
            [
                RawStreamTimeEvent(
                    records=[
                        RawTimeRecord(
                            collection=str(),
                            timestamp=timestamp,
                            asset_id=1,
                            company_id=1,
                        ),
                        RawTimeRecord(
                            collection=str(),
                            timestamp=timestamp + 1,
                            asset_id=1,
                            company_id=1,
                        ),
                        RawTimeRecord(
                            collection=str(),
                            timestamp=timestamp + 2,
                            asset_id=1,
                            company_id=1,
                        ),
                    ],
                    metadata=RawMetadata(
                        app_stream_id=1,
                        apps={SETTINGS.APP_KEY: RawAppMetadata(app_connection_id=1)},
                        log_type=LogType.time,
                    ),
                ).dict()
            ]
        )
        timestamp += 3

    result_event: StreamTimeEvent = tutorial001.app(event, context)[0]
    assert len(result_event.records) == 9, "records were not merged into a single event"


@pytest.mark.parametrize(
    "time_ranges, flat",
    (
        [((60, 120), (120, 180), (180, 240)), True],
        [((120, 180), (60, 120), (180, 240)), False],
    ),
)
def test_tutorial002(context, time_ranges, flat):

    event: List[Union[List, Dict]] = []
    for schedule_start, schedule_end in time_ranges:
        event.append(
            RawScheduledDataTimeEvent(
                asset_id=int(),
                interval=60,
                schedule=int(),
                schedule_start=schedule_start,
                schedule_end=schedule_end,
                app_connection=int(),
                app_stream=int(),
                company=int(),
                scheduler_type=SchedulerType.data_time,
            ).dict(by_alias=True, exclude_unset=True)
        )
    if not flat:
        event = [event]

    result_event: ScheduledDataTimeEvent = tutorial002.app(event, context)[0]

    assert result_event.start_time == 1
    assert result_event.end_time == 180
    max_schedule_value = time_ranges[-1][-1]
    assert result_event.schedule_end == max_schedule_value  # type: ignore[attr-defined]
