from corva import Api, Cache, ScheduledDepthEvent, scheduled  # <1>


@scheduled  # <3>
def scheduled_app(event: ScheduledDepthEvent, api: Api, cache: Cache):  # <2>
    return 'Hello, World!'  # <4>
