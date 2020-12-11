import json

import pytest
from pytest_mock import MockerFixture

from corva.loader.base import BaseLoader

BASE_LOADER_PATH = 'corva.loader.base'


def test__load_json_invalid_json(mocker: MockerFixture):
    event = '{}'

    mocker.patch(f'{BASE_LOADER_PATH}.json.loads', side_effect=ValueError)

    with pytest.raises(ValueError) as exc:
        BaseLoader._load_json(event=event)

    assert str(exc.value) == 'Invalid event JSON'


def test__load_json():
    event = {'key1': 'val1'}

    loaded = BaseLoader._load_json(event=json.dumps(event))

    assert loaded == event
