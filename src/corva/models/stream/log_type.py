import enum


class LogType(enum.Enum):
    time = 'time'
    depth = 'depth'

    @property
    def raw_event(self):
        from corva.models.stream.raw import RawStreamDepthEvent, RawStreamTimeEvent

        mapping = {self.time: RawStreamTimeEvent, self.depth: RawStreamDepthEvent}
        return mapping[self]

    @property
    def event(self):
        from corva.models.stream.stream import StreamDepthEvent, StreamTimeEvent

        mapping = {self.time: StreamTimeEvent, self.depth: StreamDepthEvent}
        return mapping[self]
