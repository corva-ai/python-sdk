from types import SimpleNamespace

from corva.configuration import SETTINGS
from docs_src import tutorial_2_configuration


def test_tutorial():
    event = [
        {
            "records": [{"asset_id": 0, "timestamp": 0}],
            "metadata": {
                "app_stream_id": 0,
                "apps": {SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]

    context = SimpleNamespace(client_context=None)

    tutorial_2_configuration.lambda_handler(event, context)
