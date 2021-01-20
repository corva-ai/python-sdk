from corva.app.base import BaseApp
from corva.event import Event
from corva.models.scheduled import ScheduledContext, ScheduledEventData


class ScheduledApp(BaseApp):
    group_by_field = 'app_connection_id'

    @property
    def event_loader(self):
        return

    def get_context(self, event: Event) -> ScheduledContext:
        return ScheduledContext()

    def post_process(self, context: ScheduledContext) -> None:
        for data in context.event:  # type: ScheduledEventData
            self.update_schedule_status(schedule=data.schedule, status='completed')

    def update_schedule_status(self, schedule: int, status: str) -> dict:
        response = self.api.post(path=f'scheduler/{schedule}/{status}')
        return response
