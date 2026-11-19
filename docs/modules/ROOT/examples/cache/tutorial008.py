from corva import Api, Cache, TaskEvent, task


@task(cache=True)  # <.>
def task_app(event: TaskEvent, api: Api, cache: Cache):  # <.>
    cache.set(key='str', value='text')  # <.>
    assert cache.get(key='str') == 'text'  # <.>
