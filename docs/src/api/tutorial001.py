from corva import Api, TaskEvent, task


@task
def task_app(event: TaskEvent, api: Api):
    api.get('/v2/pads')  # <1>
    api.get('/api/v1/data/provider/dataset/')  # <2>
    api.get('https://api.corva.ai/v2/pads')  # <3>
