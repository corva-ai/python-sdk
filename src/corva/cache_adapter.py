from typing import (
    Dict,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

import redis
import semver


class CacheRepositoryProtocol(Protocol):
    def set(
        self,
        key: str,
        value: str,
        ttl: int,
    ) -> None:
        ...

    def set_many(self, data: Sequence[Tuple[str, str, int]]) -> None:
        ...

    def get(self, key: str) -> Optional[str]:
        ...

    def get_many(self, keys: Sequence[str]) -> Dict[str, Optional[str]]:
        ...

    def get_all(self) -> Dict[str, str]:
        ...

    def delete(self, key: str) -> None:
        ...

    def delete_many(self, keys: Sequence[str]) -> None:
        ...

    def delete_all(self) -> None:
        ...


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

    def __init__(self, hash_name: str, client: redis.Redis):
        self.hash_name = hash_name
        self.zset_name = f'{hash_name}.EXPIREAT'
        self.client = client

    def run(self) -> bool:
        """Migrate from old Lua+ZSET per-field TTL to Redis built-in per-field TTL.

        Safe to call multiple times. Behavior:
          - If the legacy ZSET ("<hash>.EXPIREAT") does not exist → return False.
          - Requires Redis server version >= 7.4.0 (built-in per-field hash TTLs).
          - For each zset member (field → absolute ms deadline):
              • Past deadline → HDEL the field.
              • Future deadline → set per-field TTL via HPEXPIRE (milliseconds).
          - After processing completes: PERSIST the hash and DELETE the legacy ZSET.

        Returns True if migration was attempted on a supported server (i.e., legacy
        ZSET existed); otherwise False.
        """
        # Legacy structure must exist; otherwise nothing to do
        if not self.client.exists(self.zset_name):
            return False

        # Require Redis 7.4+ for per-field TTL commands
        redis_version_str = self.client.info(section="server").get("redis_version")
        if not redis_version_str:
            return False

        server_version = semver.Version.parse(version=redis_version_str)

        if server_version < self.MINIMUM_ALLOWED_REDIS_SERVER:
            return False

        from corva import Logger

        # Current server time in ms
        sec, micro = self.client.time()
        now_ms = int(sec) * 1000 + int(micro) // 1000

        # Queue all operations in a single pipeline and execute once
        pipe = self.client.pipeline()

        for field, score in self.client.zscan_iter(self.zset_name):
            # score is the absolute deadline in ms
            deadline_ms = int(float(score))
            ttl_ms = deadline_ms - now_ms
            if ttl_ms <= 0:
                pipe.hdel(self.hash_name, field)
            else:
                pipe.execute_command(
                    "HPEXPIRE", self.hash_name, ttl_ms, "FIELDS", 1, field
                )

        # Execute queued field operations (no-op if nothing queued)
        pipe.execute()

        # Remove key-level TTL and legacy ZSET now that fields are migrated
        self.client.persist(self.hash_name)
        self.client.delete(self.zset_name)

        # Migration was attempted because legacy ZSET existed
        Logger.info(f"Migration success: hash_name = '{self.hash_name}'")
        return True
