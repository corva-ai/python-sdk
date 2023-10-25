import pytest


def test_obsolete_attribute_import():
    with pytest.warns(FutureWarning):
        from corva import ScheduledEvent


def test_missing_attribute_import():
    with pytest.raises(ImportError):
        from corva import MISSING_ATTRIBUTE
