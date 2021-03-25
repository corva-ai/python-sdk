from corva import TaskEvent


def test_to_raw_task_event_is_up_to_date():
    """Verify that TestClient._to_raw_task_event is updated.

    If TaskEvent's schema changes this test will fail.
    What to do in this case?
        1. Fix logic in TestClient._to_raw_task_event to correspond to schema changes.
        2. Update task_event_schema below with output from TaskEvent.schema().
    """

    task_event_schema = {
        'title': 'TaskEvent',
        'description': 'Task event data.\n\nAttributes:\n    asset_id: asset id\n    '
        'company_id: company id\n    properties: custom task data',
        'type': 'object',
        'properties': {
            'asset_id': {'title': 'Asset Id', 'type': 'integer'},
            'company_id': {'title': 'Company Id', 'type': 'integer'},
            'properties': {'title': 'Properties', 'default': {}, 'type': 'object'},
        },
        'required': ['asset_id', 'company_id'],
        'additionalProperties': False,
    }

    assert TaskEvent.schema() == task_event_schema
