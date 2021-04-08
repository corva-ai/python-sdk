import enum
from typing import Any, Callable

import requests

from corva.api import Api
from corva.logger import setup_logging
from corva.models.task import TaskContext, TaskEvent


class TaskStatus(enum.Enum):
    fail = 'fail'
    success = 'success'


def get_task_event(api: Api, task_id: str) -> TaskEvent:
    response = api.get(path=f'v2/tasks/{task_id}')
    response.raise_for_status()

    return TaskEvent(**response.json())


def update_task_data(
    api: Api, task_id: str, status: TaskStatus, data: dict
) -> requests.Response:
    """Updates the task. Should never raise."""

    return api.put(path=f'v2/tasks/{task_id}/{status.value}', data=data)


def task_runner(fn: Callable, context: TaskContext) -> Any:
    try:
        task_event = get_task_event(api=context.api, task_id=context.event.task_id)

        with setup_logging(
            aws_request_id=context.aws_request_id,
            asset_id=task_event.asset_id,
            app_connection_id=None,
        ):
            result = fn(task_event, context.api)
    except Exception as exc:
        update_task_data(
            api=context.api,
            task_id=context.event.task_id,
            status=TaskStatus.fail,
            data={'fail_reason': str(exc)},
        )
        return

    # should never raise
    update_task_data(
        api=context.api,
        task_id=context.event.task_id,
        status=TaskStatus.success,
        data={'payload': result},
    )

    return result
