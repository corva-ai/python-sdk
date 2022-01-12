import datetime
from typing import Dict

import freezegun
import pytest

from corva import secrets
from corva.service import service
from corva.service.api_sdk import CachingApiSdk, FakeApiSdk
from corva.service.cache_sdk import FakeInternalCacheSdk


class TestRunApp:
    def test_invokes_cache_vacuum(self):
        api_sdk = FakeApiSdk()
        cache_sdk = FakeInternalCacheSdk()

        assert not cache_sdk.vacuum_called

        service.run_app(
            has_secrets=False,
            app_key='',
            api_sdk=api_sdk,
            cache_sdk=cache_sdk,
            app=lambda: None,
        )

        assert cache_sdk.vacuum_called

    def test_caches_secrets(self):
        def app1():
            assert secrets == {"name1": "value1"}

        def app2():
            assert secrets == {"name2": "value2"}

        api_sdk1 = CachingApiSdk(
            api_sdk=FakeApiSdk(secrets={'key': {"name1": "value1"}}), ttl=2
        )
        api_sdk2 = CachingApiSdk(
            api_sdk=FakeApiSdk(secrets={'key': {"name2": "value2"}}), ttl=2
        )
        cache_sdk = FakeInternalCacheSdk()

        freeze_time = datetime.datetime(year=2022, month=1, day=1)
        freeze_time_plus_1_sec = freeze_time + datetime.timedelta(seconds=1)
        freeze_time_plus_2_sec = freeze_time + datetime.timedelta(seconds=2)

        with freezegun.freeze_time(freeze_time):
            service.run_app(
                has_secrets=True,
                app_key='key',
                api_sdk=api_sdk1,
                cache_sdk=cache_sdk,
                app=app1,
            )

        with freezegun.freeze_time(freeze_time_plus_1_sec):
            service.run_app(
                has_secrets=True,
                app_key='key',
                api_sdk=api_sdk2,
                cache_sdk=cache_sdk,
                app=app1,
            )

        with freezegun.freeze_time(freeze_time_plus_2_sec):
            service.run_app(
                has_secrets=True,
                app_key='key',
                api_sdk=api_sdk2,
                cache_sdk=cache_sdk,
                app=app2,
            )

    @pytest.mark.parametrize(
        'has_secrets, expected_secrets',
        (
            pytest.param(
                False, {}, id='Does not fetch secrets if `has_secrets is False`'
            ),
            pytest.param(
                True, {'my': 'secret'}, id='Fetches secrets if `has_secrets is True`'
            ),
        ),
    )
    def test_temporarily_sets_secrets(
        self, has_secrets: bool, expected_secrets: Dict[str, str]
    ):
        def app():
            assert secrets == expected_secrets

        assert secrets == {}

        api_sdk = FakeApiSdk(secrets={'test_app_key': {"my": "secret"}})
        cache_sdk = FakeInternalCacheSdk()

        service.run_app(
            has_secrets=has_secrets,
            app_key='test_app_key',
            api_sdk=api_sdk,
            cache_sdk=cache_sdk,
            app=app,
        )

        assert secrets == {}

    def test_returns_app_result(self):
        api_sdk = FakeApiSdk()
        cache_sdk = FakeInternalCacheSdk()

        result = service.run_app(
            has_secrets=False,
            app_key='',
            api_sdk=api_sdk,
            cache_sdk=cache_sdk,
            app=lambda: 10,
        )

        assert result == 10
