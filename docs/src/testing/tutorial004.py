from corva import Api, TaskEvent, task


@task
def task_app(event: TaskEvent, api: Api):  # <.>
    return 'Hello, World!'


def test_task_app(app_runner):  # <.>
    event = TaskEvent(asset_id=0, company_id=0, app_id=int())  # <.>

    result = app_runner(task_app, event=event)  # <.>

    assert result == 'Hello, World!'  # <.>
