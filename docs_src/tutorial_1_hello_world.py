from corva import Api, Corva, Event, State

app = Corva()  # 1 initialize the app


@app.stream  # 2 add decorator with needed event type to your function
def user_job(event: Event, api: Api, state: State):
    # 3 add parameters with predefined types, that will be injected automatically

    """User's main logic function"""

    pass


def lambda_handler(event, context):
    # 4 define function that will be run by AWS lambda

    """AWS lambda handler"""

    user_job(event)  # 5 pass only event as parameter to your function call
