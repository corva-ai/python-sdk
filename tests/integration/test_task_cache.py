import pytest

from corva import Cache, get_task_cache_key, scheduled, task
from corva.configuration import SETTINGS
from corva.handlers import get_cache_key
from corva.models.scheduled.raw import RawScheduledDataTimeEvent
from corva.models.scheduled.scheduler_type import SchedulerType


@pytest.fixture
def invoke_task(context, requests_mock):
    def invoke(handler, task_id, asset_id, properties=None):
        url = f"{SETTINGS.API_ROOT_URL}/v2/tasks/{task_id}"
        requests_mock.get(
            url,
            json={
                "asset_id": asset_id,
                "company_id": 1,
                "properties": properties or {},
            },
        )
        success = requests_mock.put(f"{url}/success")

        (result,) = handler({"task_id": task_id, "version": 2}, context)

        assert success.called_once
        return result

    return invoke


def test_task_cache_persists_for_same_app_and_asset(invoke_task, monkeypatch):
    provider = SETTINGS.PROVIDER
    monkeypatch.setattr(SETTINGS, "APP_KEY", f"{provider}.writer")

    @task(cache=True)
    def task_app(event, api, cache):
        if "value" in event.properties:
            cache.set(key="state", value=event.properties["value"])
        return cache.get(key="state")

    assert invoke_task(task_app, "1", 101, {"value": "written"}) == "written"
    assert invoke_task(task_app, "2", 101) == "written"
    assert invoke_task(task_app, "3", 102) is None

    monkeypatch.setattr(SETTINGS, "APP_KEY", f"{provider}.other")
    assert invoke_task(task_app, "4", 101) is None


def test_task_and_scheduled_apps_read_each_others_cache(
    context, invoke_task, monkeypatch, requests_mock
):
    provider = SETTINGS.PROVIDER
    task_app_key = f"{provider}.task-app"
    scheduled_app_key = f"{provider}.scheduled-app"
    asset_id = 101
    app_stream_id = 12
    app_connection_id = 34
    scheduled_event = RawScheduledDataTimeEvent(
        asset_id=asset_id,
        company=1,
        interval=60,
        schedule=1,
        schedule_start=60,
        app_connection=app_connection_id,
        app_stream=app_stream_id,
        scheduler_type=SchedulerType.data_time,
    ).model_dump(by_alias=True, exclude_unset=True)
    completed = requests_mock.post(f"{SETTINGS.API_ROOT_URL}/scheduler/1/completed")

    @task(cache=True)
    def task_writer(event, api, cache):
        cache.set(key="state", value="from-task")

    @scheduled
    def scheduled_app(event, api, cache):
        task_cache = Cache(
            hash_name=get_task_cache_key(
                provider=provider, asset_id=event.asset_id, app_key=task_app_key
            ),
            redis_dsn=SETTINGS.CACHE_URL,
        )
        assert task_cache.get(key="state") == "from-task"

        cache.set(key="state", value="from-scheduled")
        assert task_cache.get(key="state") == "from-task"

    @task(
        cache=True,
        cache_hash_name_builder=lambda event: get_cache_key(
            provider=provider,
            asset_id=event.asset_id,
            app_stream_id=app_stream_id,
            app_key=scheduled_app_key,
            app_connection_id=app_connection_id,
        ),
    )
    def task_reader(event, api, cache):
        return cache.get(key="state")

    monkeypatch.setattr(SETTINGS, "APP_KEY", task_app_key)
    invoke_task(task_writer, "1", asset_id)

    monkeypatch.setattr(SETTINGS, "APP_KEY", scheduled_app_key)
    scheduled_app([[scheduled_event]], context)
    assert completed.called_once

    monkeypatch.setattr(SETTINGS, "APP_KEY", task_app_key)
    assert invoke_task(task_reader, "2", asset_id) == "from-scheduled"


@pytest.mark.parametrize(
    "decorate",
    [pytest.param(task, id="bare"), pytest.param(task(), id="factory")],
)
def test_task_without_cache_accepts_two_arguments(invoke_task, decorate):
    @decorate
    def task_app(event, api):
        return event.asset_id

    assert invoke_task(task_app, "1", 101) == 101
