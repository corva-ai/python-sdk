from corva.settings import CORVA_SETTINGS
from docs_src import tutorial_2_configuration


def test_tutorial():
    event = (
                '[{"records": [{"timestamp": 0, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
                '"data": {}}], "metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}, '
                '"asset_id": 0}]'
            ) % CORVA_SETTINGS.APP_KEY

    tutorial_2_configuration.lambda_handler(event, None)
