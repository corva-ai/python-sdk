from corva import Api, TaskEvent, task


@task
def task_app(event: TaskEvent, api: Api):
    api.get('/v2/pads', headers={'header': 'header-value'})  # <1>
    api.get('/v2/pads', timeout=5)  # <2>
