import contextlib
import functools

from corva.api import Api
from corva.handlers.base import base_handler
from corva.logger import setup_logging
from corva.models.task import RawTaskEvent, TaskStatus


def scheduled(func):
    @functools.wraps(func)
    @base_handler
    def wrapper(event: RawTaskEvent, api: Api, aws_request_id: str):
        status = TaskStatus.success
        data = {}
        result = None
        try:
            app_event = event.get_task_event(api=api)

            with setup_logging(
                aws_request_id=aws_request_id,
                asset_id=app_event.asset_id,
                app_connection_id=None,
            ):
                result = fn(app_event, api)
        except Exception as exc:
            status = TaskStatus.fail
            data = {'fail_reason': str(exc)}
        else:
            status = TaskStatus.success
            data = {'payload': result}

        with contextlib.suppress(Exception):
            event.update_task_data(
                api=api,
                status=status,
                data=data,
            )

        return result

    return wrapper
