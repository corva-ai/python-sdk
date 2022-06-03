from corva import ScheduledDataTimeEvent
from docs.modules.ROOT.examples.cache import (
    tutorial001,
    tutorial002,
    tutorial003,
    tutorial004,
    tutorial005,
    tutorial006,
    tutorial007,
)


def test_tutorial001(app_runner):
    event = ScheduledDataTimeEvent(asset_id=0, start_time=0, end_time=0, company_id=0)

    app_runner(tutorial001.scheduled_app, event)


def test_tutorial002(app_runner):
    event = ScheduledDataTimeEvent(asset_id=0, start_time=0, end_time=0, company_id=0)

    app_runner(tutorial002.scheduled_app, event)


def test_tutorial003(app_runner):
    event = ScheduledDataTimeEvent(asset_id=0, start_time=0, end_time=0, company_id=0)

    app_runner(tutorial003.scheduled_app, event)


def test_tutorial004(app_runner):
    event = ScheduledDataTimeEvent(asset_id=0, start_time=0, end_time=0, company_id=0)

    app_runner(tutorial004.scheduled_app, event)


def test_tutorial005(app_runner):
    event = ScheduledDataTimeEvent(asset_id=0, start_time=0, end_time=0, company_id=0)

    app_runner(tutorial005.scheduled_app, event)


def test_tutorial006(app_runner):
    event = ScheduledDataTimeEvent(asset_id=0, start_time=0, end_time=0, company_id=0)

    app_runner(tutorial006.scheduled_app, event)


def test_tutorial007(app_runner):
    event = ScheduledDataTimeEvent(asset_id=0, start_time=0, end_time=0, company_id=0)

    app_runner(tutorial007.scheduled_app, event)
