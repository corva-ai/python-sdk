import pytest

from corva.handlers import _merge_events
from corva.models.task import RawTaskEvent


def test_events_not_merged_on_unexpected_event_type():
    """
    when unexpected event type(in our test - raw task event) is
    passed - fail with RuntimeError
    """
    aws_event = [{"sample": 1}, {"sample2": 2}]
    with pytest.raises(RuntimeError):
        _merge_events(aws_event, RawTaskEvent)
