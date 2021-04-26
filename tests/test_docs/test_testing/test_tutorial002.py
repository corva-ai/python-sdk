from docs.src.testing import tutorial002


def test_tutorial002(app_runner):
    tutorial002.test_stream_depth_app(app_runner)
