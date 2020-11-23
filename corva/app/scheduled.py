from corva.app.base import BaseApp, ProcessResult
from corva.event.base import BaseEvent
from corva.event.data.scheduled import ScheduledEventData
from corva.event.scheduled import ScheduledEvent


class ScheduledApp(BaseApp):
    event_cls = ScheduledEvent

    def post_process(self, event: BaseEvent, **kwargs) -> ProcessResult:
        event = super(ScheduledApp, self).post_process(event=event, **kwargs).event
        for data in event:  # type: ScheduledEventData
            self.update_schedule_status(schedule=data.schedule, status='completed')
        return ProcessResult(event=event)

    def update_schedule_status(self, schedule: int, status: str) -> dict:
        response = self.api.post(path=f'scheduler/{schedule}/{status}')
        return response
