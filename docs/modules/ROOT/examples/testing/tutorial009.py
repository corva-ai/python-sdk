from corva import Api, Cache, TaskEvent, task


@task(cache=True)
def task_counter(event: TaskEvent, api: Api, cache: Cache):  # <.>
    counter = int(cache.get(key='counter') or 0) + 1
    cache.set(key='counter', value=str(counter))

    return counter


def test_task_counter(app_runner):  # <.>
    event = TaskEvent(asset_id=0, company_id=0)  # <.>

    cache = Cache(
        hash_name='hash_name', redis_dsn='redis://localhost', use_fakes=True
    )  # <.>

    for expected_counter in [1, 2, 3]:
        result = app_runner(task_counter, event, cache=cache)  # <.>
        assert result == expected_counter  # <.>
