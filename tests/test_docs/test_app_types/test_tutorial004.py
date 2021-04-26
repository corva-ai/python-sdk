from corva.models.task import TaskEvent
from docs.src.app_types import tutorial004


def test_tutorial002(app_runner):
    event = TaskEvent(asset_id=0, company_id=0)

    assert app_runner(tutorial004.task_app, event) == 'Hello, World!'
