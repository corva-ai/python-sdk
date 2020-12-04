from corva.event.data.base import BaseEventData
from corva.event.event import Event


def test_iter():
    data1 = BaseEventData()
    data2 = BaseEventData(a=1)
    e2 = Event(data=[data1, data2])
    for idx, data in enumerate(e2):
        assert e2.data[idx] == data


def test_eq():
    class CustomEventData(BaseEventData):
        pass

    # different len
    e1 = Event(data=[])
    e2 = Event(data=[BaseEventData()])
    assert e1 != e2

    # different fields
    e1 = Event(data=[BaseEventData()])
    e2 = Event(data=[BaseEventData(a=1)])
    assert e1 != e2

    # different order
    e1 = Event(data=[BaseEventData(), BaseEventData(a=1)])
    e2 = Event(data=[BaseEventData(a=1), BaseEventData()])
    assert e1 != e2

    # different types
    e1 = Event(data=[CustomEventData()])
    e2 = Event(data=[BaseEventData()])
    assert e1 != e2

    # success
    e1 = Event(data=[BaseEventData(), CustomEventData(a=1)])
    e2 = Event(data=[BaseEventData(), CustomEventData(a=1)])
    assert e1 == e2


def test_len():
    assert len(Event(data=[])) == 0
    assert len(Event(data=[BaseEventData()])) == 1
    assert len(Event(data=[BaseEventData(), BaseEventData()])) == 2