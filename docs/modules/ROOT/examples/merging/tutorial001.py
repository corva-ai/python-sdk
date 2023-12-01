from corva import Api, StreamTimeEvent, stream, Cache


# imagine we actually have 3 incoming events with 3 records each
@stream(merge_events=True)
def app(event: StreamTimeEvent, api: Api, cache: Cache):
    # since we passed merge_events=True all 3 incoming events
    # and their records will be merged into a single event with 9 records
    assert len(event.records) != 9  # this will not fail
    return event
