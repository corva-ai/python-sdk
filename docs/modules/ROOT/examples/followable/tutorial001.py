from corva import Api, Cache, ScheduledDataTimeEvent, StreamTimeEvent, scheduled, stream


@scheduled  # <.>
def followable_scheduled_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    data = [
        {
            'asset_id': event.asset_id,
            'version': 1,
            'timestamp': 0,
            'data': {'answer': 10},
        },
        {
            'asset_id': event.asset_id,
            'version': 1,
            'timestamp': 60,
            'data': {'answer': 11},
        },
    ]  # <.>

    # <.>
    api.insert_data(
        provider='my-provider',
        dataset='quiz-answers',
        data=data,
    )  # <.>
    api.produce_messages(data=data)  # <.>

    # <.>
    api.insert_data(
        provider='my-provider',
        dataset='quiz-answers',
        data=data,
        produce=True,  # <.>
    )


@scheduled  # <.>
def following_scheduled_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    data = api.get_dataset(
        provider='my-provider',
        dataset='quiz-answers',
        query={'asset_id': event.asset_id, 'company_id': event.company_id},
        sort={'timestamp': 1},
        limit=2,
    )  # <.>

    assert [datum['data'] for datum in data] == [
        {'answer': 10},
        {'answer': 11},
    ]


@stream  # <.>
def following_stream_app(event: StreamTimeEvent, api: Api, cache: Cache):
    assert [record.data for record in event.records] == [
        {'answer': 10},
        {'answer': 11},
    ]  # <.>
