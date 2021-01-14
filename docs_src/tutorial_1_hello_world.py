from corva import Api, Cache, Corva, StreamEvent

app = Corva()  # 1 initialize the app


@app.stream  # 2 add decorator with needed event type to your function
def stream_app(event: StreamEvent, api: Api, cache: Cache):
    # 3 above, add parameters with predefined types, that will be injected automatically

    """User's main logic function"""

    pass


def lambda_handler(event, context):
    # 4 define function that will be run by AWS lambda

    """AWS lambda handler"""

    stream_app(event)  # 5 pass only event as parameter to your function call
