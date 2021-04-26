from docs.src.testing import tutorial003


def test_tutorial003(app_runner):
    tutorial003.test_scheduled_app(app_runner)
