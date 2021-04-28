from corva import Api, TaskEvent, task


@task
def task_app(event: TaskEvent, api: Api):
    response = api.get('/v2/pads')  # <1>
    api.get('/v2/pads', params={'company': 1})  # <2>

    response.json()  # <3>
