from corva import Api, Cache, Corva, StreamEvent

app = Corva()


@app.stream(filter_by_timestamp=True)
def stream_app(event: StreamEvent, api: Api, cache: Cache):
    """User's main logic function"""

    pass


def lambda_handler(event, context):
    """AWS lambda handler"""

    stream_app(event)
