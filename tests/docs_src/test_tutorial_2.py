from types import SimpleNamespace

from docs_src import tutorial_2_configuration


def test_tutorial(settings):
    event = (
                '[{"records": [{"timestamp": 0, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                '"data": {}}], "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}}]'
            ) % settings.APP_KEY
    context = SimpleNamespace(client_context=None)

    tutorial_2_configuration.lambda_handler(event, context)
