from corva import Api  # <.>
from corva import Logger, TaskEvent, task


@task
def task_app(event: TaskEvent, api: Api):
    # <.>
    Logger.debug('Debug message!')
    Logger.info('Info message!')
    Logger.warning('Warning message!')
    Logger.error('Error message!')
    try:
        0 / 0
    except ZeroDivisionError:
        Logger.exception('Exception message!')
