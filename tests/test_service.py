from corva import secrets
from corva.service import service
from corva.service.api_sdk import FakeApiSdk


class TestRunApp:
    def test_temporarily_sets_secrets(self):
        def app():
            assert secrets == {"my": "secret"}

        assert secrets == {}

        api_sdk = FakeApiSdk(secrets={'test_app_key': {"my": "secret"}})

        service.run_app(has_secrets=True, app_key='test_app_key', api_sdk=api_sdk, app=app)

        assert secrets == {}

    def test_returns_app_result(self):
        api_sdk = FakeApiSdk(secrets={})

        result = service.run_app(
            has_secrets=False, app_key='', api_sdk=api_sdk, app=lambda: 10
        )

        assert result == 10
