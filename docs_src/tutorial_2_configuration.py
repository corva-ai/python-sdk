"""
idea https://docs.celeryproject.org/en/stable/userguide/application.html#example-3-using-a-configuration-class-object
"""

from corva import Api, Corva, Event, State

app = Corva()


class Config:
    # 1 override default parameters for Corva instance

    # 2 api params
    api_url = 'api.localhost'
    api_data_url = 'api.data.localhost'
    api_key = 'api_key'
    api_app_name = 'api_app_name'

    # 3 state params
    state_url = 'redis://'
    state_params = {'param1': 'val1'}

    # 4 other params
    ...


app.config_from_object(Config)


@app.stream
def user_job(event: Event, api: Api, state: State):
    """User's main logic function"""

    pass


def lambda_handler(event, context):
    """AWS lambda handler"""

    user_job.run(event)
