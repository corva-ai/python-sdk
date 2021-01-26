from corva import Api, Cache, Corva, StreamEvent


# 1 define a function with required parameters, that will be provided by sdk
def stream_app(event: StreamEvent, api: Api, cache: Cache):
    """Main logic function"""
    pass


# 2 define a function that will be run by AWS lambda
def lambda_handler(event, context):
    """AWS lambda handler"""
    corva = Corva()  # 3 initialize Corva
    corva.stream(stream_app, event)  # 4 run stream_app by passing it to Corva.stream alongside with event
