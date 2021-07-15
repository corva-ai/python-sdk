import sentry_sdk  # <.>

from corva import Api, TaskEvent, task

sentry_sdk.init("YOUR_SENTRY_DSN")  # <.>


@task
def app(event: TaskEvent, api: Api) -> None:
    1 / 0  # <.>
