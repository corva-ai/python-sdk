from types import SimpleNamespace

import pytest

from corva.application import Corva


def test_api_key_in_context(corva_context):
    corva = Corva(context=corva_context)

    assert corva.api.api_key == corva_context.client_context.env['API_KEY']


@pytest.mark.parametrize(
    'context',
    (
        SimpleNamespace(),
        SimpleNamespace(client_context=None),
        SimpleNamespace(client_context=SimpleNamespace(env={})),
    ),
    ids=['no client_context', 'no env', 'empty env'],
)
def test_wrong_client_context(context):
    with pytest.raises(Exception) as exc:
        Corva(context=context)

    assert str(exc.value) == 'No API Key found.'
