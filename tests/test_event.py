from corva.models.base import BaseEventData
from corva.event import Event


def test_iter():
    data1 = BaseEventData()
    data2 = BaseEventData(a=1)
    e2 = Event([data1, data2])
    for idx, data in enumerate(e2):
        assert e2.data[idx] == data


def test_eq():
    class CustomEventData(BaseEventData):
        pass

    # different len
    e1 = Event([])
    e2 = Event([BaseEventData()])
    assert e1 != e2

    # different fields
    e1 = Event([BaseEventData()])
    e2 = Event([BaseEventData(a=1)])
    assert e1 != e2

    # different order
    e1 = Event([BaseEventData(), BaseEventData(a=1)])
    e2 = Event([BaseEventData(a=1), BaseEventData()])
    assert e1 != e2

    # different types
    e1 = Event([CustomEventData()])
    e2 = Event([BaseEventData()])
    assert e1 != e2

    # success
    e1 = Event([BaseEventData(), CustomEventData(a=1)])
    e2 = Event([BaseEventData(), CustomEventData(a=1)])
    assert e1 == e2


def test_len():
    assert len(Event([])) == 0
    assert len(Event([BaseEventData()])) == 1
    assert len(Event([BaseEventData(), BaseEventData()])) == 2
