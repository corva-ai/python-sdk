def get_state_key(asset_id: int, app_stream_id: int, app_key: str, app_connection_id: int):
    provider = get_provider(app_key=app_key)
    state_key = f'{provider}/well/{asset_id}/stream/{app_stream_id}/{app_key}/{app_connection_id}'
    return state_key


def get_provider(app_key: str) -> str:
    return app_key.split('.')[0]
