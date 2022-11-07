from corva import Api, Cache, ScheduledDataTimeEvent, ScheduledDepthEvent, scheduled


@scheduled
def scheduled_time_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    api.produce_messages(data=[{'timestamp': 1}, {'timestamp': 2}])  # <.>


@scheduled
def scheduled_depth_app(event: ScheduledDepthEvent, api: Api, cache: Cache):
    api.produce_messages(data=[{'measured_depth': 1.0}, {'measured_depth': 2.0}])  # <.>
