import enum


class LogType(enum.Enum):
    time = 'time'
    depth = 'depth'

    @property
    def raw_event(self):
        from corva.models.stream.raw import RawStreamDepthEvent, RawStreamTimeEvent

        mapping = {
            type(self).time: RawStreamTimeEvent,
            type(self).depth: RawStreamDepthEvent,
        }
        return mapping[self]

    @property
    def event(self):
        from corva.models.stream.stream import StreamDepthEvent, StreamTimeEvent

        mapping = {type(self).time: StreamTimeEvent, type(self).depth: StreamDepthEvent}
        return mapping[self]
