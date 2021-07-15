import rollbar.logger  # <.>
from corva import Api, TaskEvent, task

rollbar_handler = rollbar.logger.RollbarHandler('YOUR_ROLLBAR_ACCESS_TOKEN')  # <.>


@task(handler=rollbar_handler)  # <.>
def app(event: TaskEvent, api: Api) -> None:
    1 / 0  # <.>
