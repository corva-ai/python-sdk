from typing import (
    Dict,
    Optional,
    Sequence,
    Tuple,
)

import redis
import semver


class RedisRepository:
    def __init__(self, hash_name: str, client: redis.Redis):
        self.hash_name = hash_name
        self.client = client

    def set(self, key: str, value: str, ttl: int) -> None:
        self.set_many(data=[(key, value, ttl)])

    def set_many(self, data: Sequence[Tuple[str, str, int]]) -> None:
        pipe = self.client.pipeline()
        for key, value, ttl in data:
            pipe.hset(self.hash_name, key, value)
            pipe.execute_command("HEXPIRE", self.hash_name, ttl, "FIELDS", 1, key)
        pipe.execute()

    def get(self, key: str) -> Optional[str]:
        val = self.client.hget(self.hash_name, key)
        return None if val is None else str(val)

    def get_many(self, keys: Sequence[str]) -> Dict[str, Optional[str]]:
        if keys:
            values = self.client.hmget(self.hash_name, keys)
            # redis-py returns a list of values where non-existent/expired are None
            return {k: (None if v is None else str(v)) for k, v in zip(keys, values)}
        return {}

    def get_all(self) -> Dict[str, str]:
        raw = self.client.hgetall(self.hash_name)
        return dict(raw)

    def delete(self, key: str) -> None:
        self.client.hdel(self.hash_name, key)

    def delete_many(self, keys: Sequence[str]) -> None:
        if keys:
            self.client.hdel(self.hash_name, *keys)

    def delete_all(self) -> None:
        self.client.delete(self.hash_name)


class HashMigrator:
    MINIMUM_ALLOWED_REDIS_SERVER = semver.Version(major=7, minor=4, patch=0)
    NEW_HASH_PREFIX = "migrated/"

    def __init__(self, hash_name: str, client: redis.Redis):
        self.hash_name = hash_name
        self.zset_name = f"{hash_name}.EXPIREAT"
        self.client = client

    def check_redis_server_version(self) -> None:
        # Require Redis 7.4+ for per-field TTL commands
        redis_version_str = self.client.info(section="server")["redis_version"]
        server_version = semver.Version.parse(version=redis_version_str)

        if server_version < self.MINIMUM_ALLOWED_REDIS_SERVER:
            from importlib.metadata import version

            raise RuntimeError(
                f"Redis server version {server_version} "
                f"less then {self.MINIMUM_ALLOWED_REDIS_SERVER} -> "
                f"incompatible with used python SDK version `{version('corva-sdk')}`")

    def run(self) -> bool:
        """Prepare parallel new-style cache while keeping legacy structures.

        Behavior (idempotent):
          - If legacy ZSET ("<hash>.EXPIREAT") does not exist → return False.
          - Requires Redis server >= 7.4.0 for per-field hash TTL commands.
          - Creates a new hash key with prefix NEW_HASH_PREFIX + hash_name.
          - For each zset member (field → absolute ms deadline):
              • Past deadline → do not copy field to new hash
              • Future deadline → copy current value from legacy hash to new hash and
                set per-field TTL via HPEXPIRE (milliseconds) on the new hash.
          - Legacy hash and legacy ZSET are preserved intact to allow rollback.

        Returns True if the new-style cache was created during this run
        """
        self.check_redis_server_version()

        from corva import Logger

        # Legacy structure must exist; otherwise nothing to do
        if not self.client.exists(self.zset_name):
            return False

        new_hash_name = self.NEW_HASH_PREFIX + self.hash_name

        # If new hash already exists, consider migration already done
        if self.client.exists(new_hash_name):
            return False

        # Current server time in ms
        sec, micro = self.client.time()
        now_ms = int(sec) * 1000 + int(micro) // 1000

        # Create pipeline for batched ops on the NEW hash
        pipe = self.client.pipeline()

        # Ensure the new hash key exists
        # Copy fields from old hash into the new one based on ZSET deadlines
        for field, score in self.client.zscan_iter(self.zset_name):
            # score is the absolute deadline in ms
            deadline_ms = int(float(score))
            remaining_ttl_ms = deadline_ms - now_ms
            if remaining_ttl_ms <= 0:
                continue

            value = self.client.hget(self.hash_name, field)
            if value is None:
                # No value to copy (may have been removed already)
                continue

            # Write to the new hash and apply per-field TTL there
            pipe.hset(new_hash_name, field, value)
            pipe.execute_command(
                "HPEXPIRE", new_hash_name, remaining_ttl_ms, "FIELDS", 1, field
            )

        # Execute queued field operations (no-op if nothing queued)
        pipe.execute()

        # Do NOT modify/persist legacy structures; keep them for rollback
        Logger.info(
            f"Migration done in parallel cache way: legacy='{self.hash_name}', "
            f"new='{new_hash_name}'"
        )
        return True
