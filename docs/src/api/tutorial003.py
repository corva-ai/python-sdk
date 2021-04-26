from corva import Api, TaskEvent, task


@task
def task_app(event: TaskEvent, api: Api):
    api.post('/v2/pads', data={'key': 'val'})  # <1> <5>
    api.delete('/v2/pads/123')  # <2>
    api.put('/api/v1/data/provider/dataset/', data={'key': 'val'})  # <3> <5>
    api.patch('/v2/pads/123', data={'key': 'val'})  # <4> <5>
