import traceback
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel

from corva.app.base import BaseApp
from corva.app.context import TaskContext
from corva.event.event import Event
from corva.event.loader.task import TaskLoader


class TaskData(BaseModel):
    id: str
    state: Literal['running', 'failed', 'succeeded']
    fail_reason: Optional[Any] = None
    asset_id: int
    company_id: int
    app_id: int
    document_bucket: str
    properties: Dict[str, Any]
    payload: Dict[str, Any]


class UpdateTaskInfoData(BaseModel):
    fail_reason: Optional[str] = None
    payload: dict = {}


class TaskApp(BaseApp):
    group_by_field = 'task_id'

    def event_loader(self) -> TaskLoader:
        return TaskLoader()

    def get_context(self, event: Event) -> TaskContext:
        if event[0].version != 2:
            raise Exception('Only task API v2 supported')

        task_data = self.get_task_data(task_id=event[0].task_id)

        return TaskContext(event=event, task=task_data)

    def pre_process(self, context: TaskContext) -> None:
        pass

    def process(self, context: TaskContext) -> None:
        pass

    def post_process(self, context: TaskContext) -> None:
        self.update_task_data(
            task_id=context.task.id,
            status='success',
            data=UpdateTaskInfoData(payload=context.save_data)
        )

    def on_fail(self, context: TaskContext, exception: Exception) -> None:
        data = UpdateTaskInfoData(
            fail_reason=str(exception),
            payload={'error': ''.join(traceback.TracebackException.from_exception(exception).format())}
        )
        self.update_task_data(
            task_id=context.task.id,
            status='fail',
            data=data
        )

    def get_task_data(self, task_id: str) -> TaskData:
        response = self.api.get(path=f'v2/tasks/{task_id}')
        return TaskData(**response.data)

    def update_task_data(self, task_id: str, status: Literal['fail', 'success'], data: UpdateTaskInfoData):
        return self.api.put(path=f'v2/tasks/{task_id}/{status}', json=data.dict())
