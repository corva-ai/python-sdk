from corva import Api, TaskEvent, secrets, task


@task
def task_app(event: TaskEvent, api: Api):  # <.>
    api_token = secrets['api_token']

    return api_token


def test_task_app(app_runner):  # <.>
    event = TaskEvent(asset_id=0, company_id=0, app_id=0)  # <.>

    api_token = app_runner(task_app, event, secrets={'api_token': '12345'})  # <.>

    assert api_token == '12345'  # <.>
