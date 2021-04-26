import inspect

from corva.models.task import TaskEvent
from docs.src.logging import tutorial001


def test_tutorial001(app_runner, capfd):
    event = TaskEvent(asset_id=0, company_id=0)

    app_runner(tutorial001.task_app, event)

    expected = inspect.cleandoc(
        """
    Warning message!
    Error message!
    Exception message!
    Traceback (most recent call last):
      File "/home/oleksii/Corva/python-sdk/docs/src/logging/tutorial001.py", line 13, in task_app
        0 / 0
    ZeroDivisionError: division by zero
    """
    )

    assert inspect.cleandoc(capfd.readouterr().err) == expected
