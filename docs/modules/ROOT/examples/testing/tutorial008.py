from corva import ScheduledDataTimeEvent, scheduled
from corva.service.cache_sdk import UserRedisSdk


@scheduled
def scheduled_fibonacci(event, api, cache):  # <.>
    number1 = int(cache.get('number1') or 1)
    number2 = int(cache.get('number2') or 1)
    number3 = number1 + number2
    cache.set('number1', number2)
    cache.set('number2', number3)

    return number3


def test_reset_cache(app_runner):  # <.>
    event = ScheduledDataTimeEvent(
        asset_id=0, company_id=0, start_time=0, end_time=0
    )  # <.>

    for _ in range(5):
        result = app_runner(scheduled_fibonacci, event)  # <.>
        assert result == 2  # <.>


def test_reuse_cache(app_runner):  # <.>
    event = ScheduledDataTimeEvent(
        asset_id=0, company_id=0, start_time=0, end_time=0
    )  # <.>

    cache = UserRedisSdk(
        hash_name='hash_name', redis_dsn='redis://localhost', use_fakes=True
    )  # <.>

    expected_results = [2, 3, 5, 8, 13, 21, 34, 55]
    for expected_result in expected_results:
        result = app_runner(scheduled_fibonacci, event, cache=cache)  # <.>
        assert result == expected_result  # <.>
