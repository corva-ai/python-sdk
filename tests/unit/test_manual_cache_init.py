from corva.service.cache_sdk import UserRedisSdk


def test_manual_cache_init():
    """
    When redis_client is not provided UserRedisSdk need to initialize client
    """
    cache = UserRedisSdk("test", "redis://localhost:6379", redis_client=None)
    assert cache.cache_repo.client is not None
