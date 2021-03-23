from typing import Callable

import requests

from corva.api import Api
from corva.models.task import TaskContext, TaskEvent, TaskStatus


def get_task_data(api: Api, task_id: str) -> TaskEvent:
    response = api.get(path=f'v2/tasks/{task_id}')
    response.raise_for_status()

    return TaskEvent(**response.json())


def update_task_data(
    api: Api, task_id: str, status: TaskStatus, data: dict
) -> requests.Response:
    """Updates the task. Should never raise."""

    return api.put(path=f'v2/tasks/{task_id}/{status}', data=data)


def task_runner(fn: Callable, context: TaskContext):
    try:
        task_data = get_task_data(api=context.api, task_id=context.event.task_id)

        result = fn(task_data, context.api)
    except Exception as exc:
        update_task_data(
            api=context.api,
            task_id=context.event.task_id,
            status=TaskStatus.fail.value,
            data={'fail_reason': str(exc)},
        )
        return

    # should never raise
    update_task_data(
        api=context.api,
        task_id=context.event.task_id,
        status=TaskStatus.success.value,
        data={'payload': result},
    )

    return result
