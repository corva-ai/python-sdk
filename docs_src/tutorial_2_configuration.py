from corva import Api, Cache, Corva, StreamEvent


def stream_app(event: StreamEvent, api: Api, cache: Cache):
    """Main logic function"""

    pass


def lambda_handler(event, context):
    """AWS lambda handler"""

    corva = Corva()
    corva.stream(stream_app, event, filter_by_timestamp=True)
