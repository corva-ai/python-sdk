from corva.service.cache_sdk import UserRedisSdk, InternalRedisSdk
import pytest


@pytest.mark.parametrize(
    'cache_class',
    (
        UserRedisSdk,
        InternalRedisSdk
    ),
)
def test_manual_cache_init(cache_class):
    """
    When redis_client is not provided our cache classes need to initialize client by themselves
    """
    cache = cache_class(
        "test",
        "redis://localhost:6379",
        redis_client=None
    )
    assert cache.cache_repo.client is not None
