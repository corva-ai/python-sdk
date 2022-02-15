from datetime import timedelta
from typing import Dict, List, Optional, Protocol, Union

import redis


class CacheRepositoryProtocol(Protocol):
    def set(
        self,
        key: str,
        value: str,
        ttl: int,
    ) -> None:
        ...

    def get(self, key: str) -> Optional[str]:
        ...

    def delete(self, key: str) -> None:
        ...


class RedisRepository:
    # Inserts a field with an expiration time into the hash specified by the key.
    #
    # Complexity: O(log(N)) with N being the number of elements in the hash.
    #
    # 1. If hash does not exist, it will automatically create one.
    # 2. If the field already exists, its value and ttl will be overwritten.
    # 3. Hash ttl is always set to the biggest field's ttl.
    # 4. When the field expires it may be deleted by:
    #    - Manually invoking `vacuum` script.
    #    - Redis automatically deleting expired hash (see note #3).
    #
    # Args:
    #     KEYS:
    #         hash_name.
    #         zset_name.
    #
    #     ARGV:
    #         key.
    #         value.
    #         ttl.
    #
    # Returns: nil.
    LUA_SET_SCRIPT = """
    local hash_name = KEYS[1]
    local zset_name = KEYS[2]
    local key = ARGV[1]
    local value = ARGV[2]
    local ttl = tonumber(ARGV[3])
    local time = redis.call('TIME')
    local pexpireat = (
            (tonumber(time[1]) + ttl) * 1000 + math.floor(tonumber(time[2]) / 1000)
    )

    redis.call('HSET', hash_name, key, value)
    redis.call('ZADD', zset_name, pexpireat, key)

    local max_pexpireat = tonumber(redis.call(
            'ZREVRANGEBYSCORE', zset_name, '+inf', '-inf', 'WITHSCORES', 'LIMIT', 0, 1
    )[2])

    redis.call('PEXPIREAT', hash_name, max_pexpireat)
    redis.call('PEXPIREAT', zset_name, max_pexpireat)
    """

    # Gets the value of a field in hash specified by the key.
    #
    # Complexity: O(1).
    #
    # Args:
    #     KEYS:
    #         hash_name.
    #         zset_name.
    #
    #     ARGV:
    #         key.
    #
    # Returns:
    #     - nil if the field has expired.
    #     - field's value when field does not have expiration time.
    #     - field's value when field has not expired yet.
    LUA_GET_SCRIPT = """
    local hash_name = KEYS[1]
    local zset_name = KEYS[2]
    local key = ARGV[1]
    local time = redis.call('TIME')
    local pnow = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
    local pexpireat = redis.call('ZSCORE', zset_name, key)

    if not pexpireat or pnow < tonumber(pexpireat) then
        return redis.call('HGET', hash_name, key)
    end
    """

    # Deletes M expired keys from the hash.
    #
    # Complexity: O(log(N) + M) + O(M) + O(M * log(N)) with N being the number of
    # elements in the hash and M being the number of elements deleted. For constant M
    # (e.g., 3) the complexity is O(log(N)).
    #
    # Args:
    #     KEYS:
    #         hash_name.
    #         zset_name.
    #
    #     ARGV:
    #         delete_count.
    #
    # Returns: nil.
    LUA_VACUUM_SCRIPT = """
    local hash_name = KEYS[1]
    local zset_name = KEYS[2]
    local delete_count = tonumber(ARGV[1])
    local time = redis.call('TIME')
    local pnow = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

    local keys_to_delete = redis.call(
            'ZRANGEBYSCORE', zset_name, '-inf', pnow, 'LIMIT', 0, delete_count
    )

    if not next(keys_to_delete) then
        return
    end

    redis.call('HDEL', hash_name, unpack(keys_to_delete))
    redis.call('ZREM', zset_name, unpack(keys_to_delete))
    """

    def __init__(self, hash_name: str, client: redis.Redis):
        self.hash_name = hash_name
        self.zset_name = f'{hash_name}.EXPIREAT'
        self.client = client
        self.lua_set = self.client.register_script(self.LUA_SET_SCRIPT)
        self.lua_get = self.client.register_script(self.LUA_GET_SCRIPT)
        self.lua_vacuum = self.client.register_script(self.LUA_VACUUM_SCRIPT)

    def set(self, key: str, value: str, ttl: int) -> None:
        self.lua_set(keys=[self.hash_name, self.zset_name], args=[key, value, ttl])

    def get(self, key: str) -> Optional[str]:
        return self.lua_get(keys=[self.hash_name, self.zset_name], args=[key])

    def delete(self, key: str) -> None:
        self.set(key=key, value='', ttl=-1)

    def vacuum(self, delete_count: int) -> None:
        self.lua_vacuum(keys=[self.hash_name, self.zset_name], args=[delete_count])


class DeprecatedRedisAdapter:
    """Expands basic redis functionality

    Serves the purpose of adding custom logic on top of basic redis functions
    (e.g. adding expiry to hset, which is not available out of the box).
    """

    DEFAULT_EXPIRY: timedelta = timedelta(days=60)
    REDIS_STORED_VALUE_TYPE = Union[bytes, str, int, float]

    def __init__(
        self,
        hash_name: str,
        client: redis.Redis,
    ):
        self.default_name = hash_name
        self.client = client

        self.client.ping()

    def hset(
        self,
        name: Optional[str] = None,
        key: Optional[str] = None,
        value: Optional[REDIS_STORED_VALUE_TYPE] = None,
        mapping: Optional[Dict[str, REDIS_STORED_VALUE_TYPE]] = None,
        expiry: Union[int, timedelta, None] = DEFAULT_EXPIRY,
    ) -> int:
        """Stores the data in cache

        params:
         key: key, which will contain the data
         value: data to be saved
         mapping: dict of key:data pairs to be saved
         expiry: time in seconds for when data will be deleted from cache
            expiration is reset with every hset call
            expiration can be disabled by setting expiry to None
        returns: number of inserted elements
        """

        name = name or self.default_name

        n_set = self.client.hset(name=name, key=key, value=value, mapping=mapping)

        if expiry is None and self.pttl(name=name) > 0:
            self.client.persist(name=name)

        if expiry is not None:
            self.client.expire(name=name, time=expiry)

        return n_set

    def hget(
        self, key: str, name: Optional[str] = None
    ) -> Union[REDIS_STORED_VALUE_TYPE, None]:
        """Loads data from cache

        params:
         key: key to load data from
        returns: stored data
        """

        name = name or self.default_name
        return self.client.hget(name=name, key=key)

    def hgetall(
        self, name: Optional[str] = None
    ) -> Dict[str, Union[REDIS_STORED_VALUE_TYPE]]:
        """Loads all data from cache"""

        name = name or self.default_name
        return self.client.hgetall(name=name)

    def hdel(self, keys: List[str], name: Optional[str] = None) -> int:
        """Deletes some data from cache

        params:
         keys: list of keys to delete
        returns: number of deleted elements
        """

        name = name or self.default_name
        return self.client.hdel(name, *keys)

    def delete(self, *names: List[str]) -> int:
        """Deletes all data from cache"""

        names = names or [self.default_name]
        return self.client.delete(*names)

    def ttl(self, name: Optional[str] = None) -> int:
        """Returns the number of seconds until expiration"""

        name = name or self.default_name
        return self.client.ttl(name=name)

    def pttl(self, name: Optional[str] = None) -> int:
        """Returns the number of milliseconds until expiration"""

        name = name or self.default_name
        return self.client.pttl(name=name)

    def exists(self, *names: List[str]) -> int:
        """Returns whether there is data in cache"""

        names = names or [self.default_name]
        return self.client.exists(*names)
