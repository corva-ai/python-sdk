from corva.settings import CORVA_SETTINGS
from docs_src import tutorial_1_hello_world


def test_tutorial():
    event = (
                '[{"records": [{"asset_id": 0, "company_id": 0, "version": 0, "collection": "", "data": {}}], '
                '"metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}, "asset_id": 0}]'
            ) % CORVA_SETTINGS.APP_KEY

    tutorial_1_hello_world.lambda_handler(event, None)
