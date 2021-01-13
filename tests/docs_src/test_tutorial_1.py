from docs_src.tutorial_1_hello_world import lambda_handler


def test_tutorial(settings):
    event = (
                '[{"records": [{"asset_id": 0, "company_id": 0, "version": 0, "collection": "", "data": {}}], '
                '"metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0}}}, "asset_id": 0}]'
            ) % settings.APP_KEY

    lambda_handler(event, None)
