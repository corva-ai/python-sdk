from typing import (
    Dict,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

import redis


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

