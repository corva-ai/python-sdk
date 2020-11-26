from corva.utils import get_provider


def test_get_app_provider():
    app_key = 'company.app-name'
    assert get_provider(app_key=app_key) == 'company'
