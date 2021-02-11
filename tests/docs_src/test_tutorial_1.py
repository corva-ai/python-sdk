from types import SimpleNamespace

from docs_src import tutorial_1_hello_world


def test_tutorial(settings):
    event = (
        '[{"records": [{"asset_id": 0, "timestamp": 0}], '
        '"metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}}]'
    ) % settings.APP_KEY
    context = SimpleNamespace(client_context=None)

    tutorial_1_hello_world.lambda_handler(event, context)
