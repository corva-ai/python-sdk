from docs.src.testing import tutorial001


def test_tutorial001(app_runner):
    tutorial001.test_stream_time_app(app_runner)
