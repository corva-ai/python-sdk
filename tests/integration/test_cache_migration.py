import time
from typing import Dict

import pytest

from corva.cache_adapter import HashMigrator


def _now_ms(redis_client) -> int:
    sec, micro = redis_client.time()
    return int(sec) * 1000 + int(micro) // 1000


def _zset_name(hash_name: str) -> str:
    return f"{hash_name}.EXPIREAT"


@pytest.mark.integration
def test_migrate_returns_false_when_no_legacy_zset_present(clean_real_redis):
    hash_name = "test:migration:nozset:" + str(time.time_ns())
    zset = _zset_name(hash_name)

    clean_real_redis.delete(hash_name, zset)
    clean_real_redis.hset(hash_name, mapping={"a": "1"})

    migrator = HashMigrator(hash_name=hash_name, client=clean_real_redis)

    # No legacy zset exists -> migration should be a no-op and return False
    assert clean_real_redis.exists(zset) == 0
    migrated = migrator.run()
    assert not migrated
    assert clean_real_redis.hget(hash_name, "a") == "1"


@pytest.mark.integration
def test_migrate_converts_fields_and_cleans_legacy_structures(clean_real_redis):
    hash_name = "test:migration:convert:" + str(time.time_ns())
    zset = _zset_name(hash_name)

    clean_real_redis.delete(hash_name, zset)

    # Prepopulate legacy hash + per-field expirations in zset (absolute ms deadlines)
    clean_real_redis.hset(hash_name, mapping={"f1": "v1", "f2": "v2"})
    now = _now_ms(clean_real_redis)
    future_deadline = now + 2000  # 2 seconds in the future
    past_deadline = now - 100     # already expired

    clean_real_redis.zadd(zset, mapping={"f1": future_deadline, "f2": past_deadline})
    # Simulate old behavior of setting key-level expiry as backstop (optional)
    clean_real_redis.pexpireat(hash_name, future_deadline)
    clean_real_redis.pexpireat(zset, future_deadline)

    migrator = HashMigrator(hash_name=hash_name, client=clean_real_redis)

    migrated = migrator.run()
    assert migrated

    # zset removed and key-level TTL removed (persisted)
    assert clean_real_redis.exists(zset) == 0
    assert clean_real_redis.ttl(hash_name) == -1

    # f1 remains and has a per-field TTL
    assert clean_real_redis.hget(hash_name, "f1") == "v1"
    f1_hpttl = clean_real_redis.execute_command("HPTTL", hash_name, "FIELDS", 1, "f1")
    if isinstance(f1_hpttl, list):
        f1_hpttl = f1_hpttl[0]
    assert isinstance(f1_hpttl, int) and f1_hpttl > 0

    # f2 was expired -> should be deleted from hash
    assert clean_real_redis.hget(hash_name, "f2") is None


@pytest.mark.integration
def test_migrate_is_idempotent(clean_real_redis):
    hash_name = "test:migration:idempotent:" + str(time.time_ns())
    zset = _zset_name(hash_name)

    clean_real_redis.delete(hash_name, zset)

    clean_real_redis.hset(hash_name, mapping={"k": "v"})
    now = _now_ms(clean_real_redis)
    clean_real_redis.zadd(zset, mapping={"k": now + 5000})

    migrator = HashMigrator(hash_name, clean_real_redis)
    migrated = migrator.run()
    assert migrated
    # Run again – zset is gone so it should be a no-op returning False
    migrated = migrator.run()
    assert not migrated


@pytest.mark.integration
def test_migrate_large_batch_processes_all_fields(clean_real_redis):
    hash_name = "test:migration:large:" + str(time.time_ns())
    zset = _zset_name(hash_name)

    clean_real_redis.delete(hash_name, zset)

    # Create 300 fields to exceed default batch=256
    mapping: Dict[str, str] = {f"k{i}": f"v{i}" for i in range(300)}
    clean_real_redis.hset(hash_name, mapping=mapping)

    now = _now_ms(clean_real_redis)
    future = now + 60_000
    zmap = {f"k{i}": future for i in range(300)}
    clean_real_redis.zadd(zset, mapping=zmap)

    migrator = HashMigrator(hash_name, clean_real_redis)

    assert migrator.run() is True

    # All fields should still be present
    assert clean_real_redis.hlen(hash_name) == 300

    # zset removed, hash persisted
    assert clean_real_redis.exists(zset) == 0
    assert clean_real_redis.ttl(hash_name) == -1

    # Sample a few fields to ensure per-field TTLs were applied
    for key in ["k0", "k128", "k256"]:
        hpttl = clean_real_redis.execute_command("HPTTL", hash_name, "FIELDS", 1, key)
        if isinstance(hpttl, list):
            hpttl = hpttl[0]
        assert isinstance(hpttl, int) and hpttl > 0
