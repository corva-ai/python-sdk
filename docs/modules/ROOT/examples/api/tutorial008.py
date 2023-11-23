from corva import Api, Cache, ScheduledDataTimeEvent, ScheduledDepthEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    api.max_retries = 5  # Enabling up to 5 retries when HTTP error happens.
    ...
