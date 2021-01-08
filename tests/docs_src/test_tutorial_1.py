from docs_src.tutorial_1_hello_world import lambda_handler


def test_tutorial(raw_stream_event):
    lambda_handler(raw_stream_event, None)
