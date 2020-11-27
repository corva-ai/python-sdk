from corva.utils import get_provider, get_state_key


def test_get_state_key(patch_base_event):
    provider = 'provider'
    app_key = f'{provider}.app-key'
    asset_id = 1
    app_stream_id = 2
    app_connection_id = 3

    state_key = get_state_key(
        asset_id=asset_id,
        app_stream_id=app_stream_id,
        app_key=app_key,
        app_connection_id=app_connection_id
    )
    assert state_key == f'{provider}/well/{asset_id}/stream/{app_stream_id}/{app_key}/{app_connection_id}'


def test_get_app_provider():
    app_key = 'company.app-name'
    assert get_provider(app_key=app_key) == 'company'
