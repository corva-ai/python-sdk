from corva import Api, TaskEvent, task  # <1>


@task  # <3>
def task_app(event: TaskEvent, api: Api):  # <2>
    return 'Hello, World!'  # <4>
