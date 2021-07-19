import logging  # <.>

from corva import Api, Logger, TaskEvent, task

stream_handler = logging.StreamHandler()  # <.>


@task(handler=stream_handler)  # <.>
def task_app(event: TaskEvent, api: Api):
    Logger.info('Info message!')  # <.>
