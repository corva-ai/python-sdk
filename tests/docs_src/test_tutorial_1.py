from docs_src import tutorial_1_hello_world


def test_tutorial(corva_settings):
    event = (
                '[{"records": [{"asset_id": 0, "company_id": 0, "version": 0, "collection": "", "data": {}}], '
                '"metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}, "asset_id": 0}]'
            ) % corva_settings.APP_KEY

    tutorial_1_hello_world.lambda_handler(event, None)
