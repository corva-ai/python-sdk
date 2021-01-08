from docs_src.tutorial_2_configuration import lambda_handler


def test_tutorial(raw_stream_event):
    lambda_handler(raw_stream_event, None)
