from typing import Dict
from unittest import mock

import pytest

from corva.cache_adapter import HashMigrator

hash_name = "/corva/well/test"
zset_name = f"{hash_name}.EXPIREAT"
new_hash_name = "/new" + hash_name


@pytest.fixture(scope="function")
def current_redis_server_time(redis_client):
    sec, micro = redis_client.time()
    return int(sec) * 1000 + int(micro) // 1000


def test_server_version_incompatible_with_sdk(redis_client):

    with (pytest.raises(RuntimeError) as exc):
        with mock.patch.object(redis_client, "info",
                               return_value={"redis_version": "6.2.0"}):
            HashMigrator(hash_name="any", client=redis_client
                         ).check_redis_server_version()
            assert "incompatible with used python SDK version" in exc


def test_migrate_returns_false_when_no_legacy_zset_present(redis_client):
    redis_client.hset(hash_name, mapping={"a": "1"})

    migrator = HashMigrator(hash_name=hash_name, client=redis_client)

    # No legacy zset exists -> migration should be a no-op and return False
    assert redis_client.exists(zset_name) == 0
    migrated = migrator.run()
    assert not migrated


def test_migrate_creates_new_hash_and_keeps_legacy(
    redis_client, current_redis_server_time
):
    redis_client.hset(hash_name, mapping={"f1": "v1", "f2": "v2"})
    future_deadline = current_redis_server_time + 2000  # 2 seconds in the future
    past_deadline = current_redis_server_time - 100  # already expired

    redis_client.zadd(zset_name, mapping={"f1": future_deadline, "f2": past_deadline})
    # Simulate old behavior of setting key-level expiry as backstop (optional)
    redis_client.pexpireat(hash_name, future_deadline)
    redis_client.pexpireat(zset_name, future_deadline)

    migrator = HashMigrator(hash_name=hash_name, client=redis_client)

    migrated = migrator.run()
    assert migrated

    # Legacy structures are preserved
    assert redis_client.exists(zset_name) == 1
    assert redis_client.ttl(hash_name) != -1  # key-level TTL remains as set

    # New hash created and populated with non-expired fields only
    assert redis_client.hget(new_hash_name, "f1") == "v1"
    field_1_ttl = redis_client.execute_command(
        "HPTTL", new_hash_name, "FIELDS", 1, "f1"
    )[0]
    assert isinstance(field_1_ttl, int) and field_1_ttl > 0

    # f2 expired -> should not be present in the new hash; legacy still has it
    assert redis_client.hget(new_hash_name, "f2") is None
    assert redis_client.hget(hash_name, "f2") == "v2"


def test_migrate_is_idempotent(redis_client, current_redis_server_time):

    redis_client.hset(hash_name, mapping={"k": "v"})
    redis_client.zadd(zset_name, mapping={"k": current_redis_server_time + 5000})

    migrator = HashMigrator(hash_name, redis_client)
    migrated = migrator.run()
    assert migrated
    # Run again – new hash exists so it should be a no-op returning False
    migrated = migrator.run()
    assert not migrated


def test_migrate_large_batch_processes_all_fields(
    redis_client, current_redis_server_time
):

    # Create 300 fields to exceed default batch=256
    mapping: Dict[str, str] = {f"k{i}": f"v{i}" for i in range(300)}
    redis_client.hset(hash_name, mapping=mapping)

    future = current_redis_server_time + 60_000
    zmap = {f"k{i}": future for i in range(300)}
    redis_client.zadd(zset_name, mapping=zmap)

    migrator = HashMigrator(hash_name, redis_client)

    assert migrator.run() is True

    # All fields should be present in the new hash
    assert redis_client.hlen(new_hash_name) == 300

    # Legacy zset remains, legacy hash unchanged
    assert redis_client.exists(zset_name) == 1

    # Sample a few fields to ensure per-field TTLs were applied on the new hash
    for key in ["k0", "k128", "k256"]:
        hpttl = redis_client.execute_command("HPTTL", new_hash_name, "FIELDS", 1, key)
        if isinstance(hpttl, list):
            hpttl = hpttl[0]
        assert isinstance(hpttl, int) and hpttl > 0
