from corva import Api, Cache, Corva, StreamEvent


# 1 define your function with essential parameters, that will be provided by Corva
def stream_app(event: StreamEvent, api: Api, cache: Cache):
    """Main logic function"""

    pass


# 2 define function that will be run by AWS lambda
def lambda_handler(event, context):
    """AWS lambda handler"""

    app = Corva()  # 3 initialize the app
    app.stream(stream_app, event)  # 4 run stream app
