from types import SimpleNamespace

import pytest
from pytest_mock import MockerFixture

from corva.application import Corva


def test_api_key_in_context(mocker: MockerFixture):
    context = SimpleNamespace(client_context=SimpleNamespace(env={"API_KEY": '123'}))

    mocker.patch('corva.configuration.SETTINGS.API_KEY', '456')

    corva = Corva(context=context)

    assert corva.api.api_key == '123'


def test_api_key_in_settings(mocker: MockerFixture):
    context = SimpleNamespace(client_context=None)

    mocker.patch('corva.configuration.SETTINGS.API_KEY', '456')

    corva = Corva(context=context)

    assert corva.api.api_key == '456'


def test_no_api_key_found(mocker: MockerFixture):
    context = SimpleNamespace(client_context=None)
    mocker.patch('corva.configuration.SETTINGS.API_KEY', None)

    with pytest.raises(Exception) as exc:
        Corva(context=context)

    assert str(exc.value) == 'No API Key found.'
