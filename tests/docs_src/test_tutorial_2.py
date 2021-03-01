from docs_src import tutorial_2_configuration


def test_tutorial(corva_context):
    event = [{"records": [{"asset_id": 0, "timestamp": 0}]}]

    tutorial_2_configuration.lambda_handler(event, corva_context)
