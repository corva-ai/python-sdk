from corva import Api, Corva, Event, State

app = Corva(
    # 1 api params
    api_url='api.localhost',
    api_data_url='api.data.localhost',
    api_key='api_key',
    api_app_name='api_app_name',

    # 2 state params
    state_url='redis://',
    state_params={'param1': 'val1'}
)


@app.stream
def user_job(event: Event, api: Api, state: State):
    """User's main logic function"""

    pass


def lambda_handler(event, context):
    """AWS lambda handler"""

    user_job(event)
