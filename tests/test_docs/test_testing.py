from docs.src.testing import tutorial001, tutorial002, tutorial003, tutorial004


def test_tutorial001(app_runner):
    tutorial001.test_stream_time_app(app_runner)


def test_tutorial002(app_runner):
    tutorial002.test_stream_depth_app(app_runner)


def test_tutorial003(app_runner):
    tutorial003.test_scheduled_app(app_runner)


def test_tutorial004(app_runner):
    tutorial004.test_task_app(app_runner)
