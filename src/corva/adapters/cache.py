import datetime
from typing import Optional, Protocol

import redis


class CacheProtocol(Protocol):
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

    def vacuum(self) -> None:
        ...


class RedisCache:
    SIXTY_DAYS: int = int(datetime.timedelta(days=60).total_seconds())

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

    # Deletes three expired keys from the hash.
    #
    # Complexity: O(log(N) + M) + O(M) + O(M * log(N)) with N being the number of
    # elements in the hash and M being number of elements deleted. For constant M == 3
    # the complexity is O(log(N)).
    #
    # Args:
    #     KEYS:
    #         hash_name.
    #         zset_name.
    #
    # Returns: nil.
    LUA_VACUUM_SCRIPT = """
    local hash_name = KEYS[1]
    local zset_name = KEYS[2]
    local time = redis.call('TIME')
    local pnow = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

    local keys_to_delete = redis.call(
            'ZRANGEBYSCORE', zset_name, '-inf', pnow, 'LIMIT', 0, 3
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

    def set(
        self,
        key: str,
        value: str,
        ttl: int = SIXTY_DAYS,
    ) -> None:
        self.lua_set(keys=[self.hash_name, self.zset_name], args=[key, value, ttl])

    def get(self, key: str) -> Optional[str]:
        return self.lua_get(keys=[self.hash_name, self.zset_name], args=[key])

    def delete(self, key: str) -> None:
        self.set(key=key, value='', ttl=-1)

    def vacuum(self) -> None:
        self.lua_vacuum(keys=[self.hash_name, self.zset_name])
