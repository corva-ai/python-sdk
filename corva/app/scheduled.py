from corva.app.base import BaseApp
from corva.app.utils.context import ScheduledContext
from corva.event.data.scheduled import ScheduledEventData
from corva.event.event import Event
from corva.event.loader.scheduled import ScheduledLoader
from corva.state.redis_adapter import RedisAdapter
from corva.state.redis_state import RedisState
from corva.utils import GetStateKey


class ScheduledApp(BaseApp):
    group_by_field = 'app_connection_id'

    def event_loader(self) -> ScheduledLoader:
        return ScheduledLoader()

    def get_context(self, event: Event) -> ScheduledContext:
        return ScheduledContext(
            event=event,
            state=RedisState(
                redis=RedisAdapter(
                    default_name=GetStateKey.from_event(event=event, app_key=self.app_key),
                    cache_url=self.cache_url,
                    logger=self.logger
                ),
                logger=self.logger
            )
        )

    def pre_process(self, context: ScheduledContext) -> None:
        pass

    def process(self, context: ScheduledContext) -> None:
        pass

    def post_process(self, context: ScheduledContext) -> None:
        for data in context.event:  # type: ScheduledEventData
            self.update_schedule_status(schedule=data.schedule, status='completed')

    def on_fail(self, context: ScheduledContext, exception: Exception) -> None:
        pass

    def update_schedule_status(self, schedule: int, status: str) -> dict:
        response = self.api.post(path=f'scheduler/{schedule}/{status}')
        return response
