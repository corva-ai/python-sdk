from corva import Api, Cache, ScheduledDataTimeEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    api.insert_data(
        provider='my-company-name',
        dataset='my-datset-name',
        data=[
            {
                'asset_id': event.asset_id,
                'version': 1,
                'timestamp': 0,
                'data': {'result': 'very important result'}
            },
            {
                'asset_id': event.asset_id,
                'version': 1,
                'timestamp': 1,
                'data': {'result': 'very important result'}
            }
        ],  # <.>
        produce=True  # <.>
    )
