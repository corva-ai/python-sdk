from corva import Api, TaskEvent, secrets, task  # <.>


@task
def task_app(event: TaskEvent, api: Api):
    secrets['api_token']  # <.>
    int(secrets['integer'])  # <.>
