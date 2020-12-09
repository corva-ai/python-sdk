from corva.event import Event


class GetStateKey:
    @classmethod
    def _get_key(cls, asset_id: int, app_stream_id: int, app_key: str, app_connection_id: int):
        provider = cls._get_provider(app_key=app_key)
        state_key = f'{provider}/well/{asset_id}/stream/{app_stream_id}/{app_key}/{app_connection_id}'
        return state_key

    @staticmethod
    def _get_provider(app_key: str) -> str:
        return app_key.split('.')[0]

    @classmethod
    def from_event(cls, event: Event, app_key: str):
        return cls._get_key(
            asset_id=event[0].asset_id,
            app_stream_id=event[0].app_stream_id,
            app_key=app_key,
            app_connection_id=event[0].app_connection_id
        )
