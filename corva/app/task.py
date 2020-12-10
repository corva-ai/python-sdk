from typing import Literal

from corva.app.base import BaseApp
from corva.event import Event
from corva.loader.task import TaskLoader
from corva.models.task import TaskData, UpdateTaskData, TaskContext
from corva.models.task import TaskStatus


class TaskApp(BaseApp):
    group_by_field = 'task_id'

    def event_loader(self) -> TaskLoader:
        return TaskLoader()

    def get_context(self, event: Event) -> TaskContext:
        task_data = self.get_task_data(task_id=event[0].task_id)

        return TaskContext(event=event, task=task_data)

    def post_process(self, context: TaskContext) -> None:
        self.update_task_data(
            task_id=context.task.id,
            status=TaskStatus.success.value,
            data=UpdateTaskData(payload=context.task_result)
        )

    def on_fail(self, context: TaskContext, exception: Exception) -> None:
        data = UpdateTaskData(fail_reason=str(exception))
        self.update_task_data(task_id=context.task.id, status=TaskStatus.fail.value, data=data)

    def get_task_data(self, task_id: str) -> TaskData:
        response = self.api.get(path=f'v2/tasks/{task_id}')
        return TaskData(**response.data)

    def update_task_data(self, task_id: str, status: Literal['fail', 'success'], data: UpdateTaskData):
        return self.api.put(path=f'v2/tasks/{task_id}/{status}', json=data.dict())
