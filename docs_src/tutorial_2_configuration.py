from corva import Api, Corva, StreamEvent, State

app = Corva()


@app.stream(filter_by_timestamp=True)
def stream_app(event: StreamEvent, api: Api, state: State):
    """User's main logic function"""

    pass


def lambda_handler(event, context):
    """AWS lambda handler"""

    stream_app(event)
