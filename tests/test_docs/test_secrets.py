from corva import TaskEvent
from docs.src.secrets import tutorial001


def test_tutorial001(app_runner):
    event = TaskEvent(asset_id=0, company_id=0, app_id=10)

    app_runner(
        tutorial001.task_app, event, secrets={'api_token': 'abc12345', 'integer': '1'}
    )
