from corva.service.cache_sdk import UserRedisSdk


def test_manual_cache_init():
    """
    When redis_client is not provided UserRedisSdk need to initialize client
    """
    cache = UserRedisSdk("test", "redis://localhost:6379", redis_client=None)
    assert cache.cache_repo.client is not None


def test__migration_not_called_on_user_redis_sdk_init__success():

    cache = UserRedisSdk("test", "redis://localhost:6379", use_fakes=True)

    assert cache._migrated is False

    cache.get("key")

    assert cache._migrated is True
