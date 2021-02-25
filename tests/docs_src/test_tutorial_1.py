from corva.configuration import SETTINGS
from docs_src import tutorial_1_hello_world


def test_tutorial(corva_context):
    event = [
        {
            "records": [{"asset_id": 0, "timestamp": 0}],
            "metadata": {
                "app_stream_id": 0,
                "apps": {SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]

    tutorial_1_hello_world.lambda_handler(event, corva_context)
