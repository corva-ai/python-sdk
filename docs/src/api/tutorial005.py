from corva import Api, TaskEvent, task


@task
def task_app(event: TaskEvent, api: Api):
    api.get_dataset(
        provider='corva',
        dataset='wits',
        query={
            'asset_id': event.asset_id,
        },
        sort={'timestamp': 1},
        limit=1,
        fields='data,metadata',
    )
