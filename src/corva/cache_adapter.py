import itertools
from datetime import timedelta
from typing import Dict, List, Optional, Protocol, Sequence, Tuple, Union

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

    def ttl(self, key) -> Optional[int]:
        ...


class RedisRepository:
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

    LUA_TTL_SCRIPT = """
    local zset_name = KEYS[1]
    local key = ARGV[1]
    local time = redis.call('TIME')
    local pnow = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
    
    local pexpireat = redis.call('ZSCORE', zset_name, key)
    
    if not pexpireat then
        return
    end
    
    local ttl = tonumber(pexpireat) - pnow
    
    if ttl <= 0 then
        return
    end
    
    return ttl
    """

    LUA_GET_ALL_SCRIPT = """
    local hash_name = KEYS[1]
    local zset_name = KEYS[2]
    local time = redis.call('TIME')
    local pnow = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
    
    local hash = redis.call('HGETALL', hash_name)
    local zset = redis.call('ZRANGEBYSCORE', zset_name, '-inf', '+inf', 'WITHSCORES')
    
    local zset_mapping = {}
    for i, _ in ipairs(zset) do
        if i % 2 == 1 then
            zset_mapping[zset[i]] = zset[i + 1]
        end
    end
    
    local result = {}
    for i, _ in ipairs(hash) do
        if i % 2 == 1 then
            local pexpireat = zset_mapping[hash[i]]
    
            if not pexpireat or pnow < tonumber(pexpireat) then
                table.insert(result, hash[i])
                table.insert(result, hash[i + 1])
            end
        end
    end
    
    return result
    """

    # Gets the values of the fields in the hash specified by the keys.
    #
    # Complexity: O(N), where N is the number of requested keys.
    #
    # Args:
    #     KEYS:
    #         hash_name.
    #         zset_name.
    #
    #     ARGV:
    #         key
    #         key
    #         ...
    #
    # Returns:
    #     - nil if the field has expired.
    #     - field's value when field does not have expiration time.
    #     - field's value when field has not expired yet.
    LUA_GET_MANY_SCRIPT = """
    local hash_name = KEYS[1]
    local zset_name = KEYS[2]
    local time = redis.call('TIME')
    local pnow = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
    
    local hash = redis.call('HMGET', hash_name, unpack(ARGV))
    
    local result = {}
    
    for i, key in ipairs(ARGV) do
        local pexpireat = redis.call('ZSCORE', zset_name, key)
    
        if not pexpireat or pnow < tonumber(pexpireat) then
            table.insert(result, hash[i])
        else
            table.insert(result, nil)
        end
    end
    
    return result
    """

    # Inserts fields with expiration times into the hash specified by the keys.
    #
    # Complexity: O(Nlog(M)), where N number of inserted elements, M hash size.
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
    LUA_SET_MANY_SCRIPT = """
    local hash_name = KEYS[1]
    local zset_name = KEYS[2]
    local time = redis.call('TIME')
    
    for i, _ in ipairs(ARGV) do
        if i % 3 == 1 then
            local key = ARGV[i]
            local value = ARGV[i + 1]
            local ttl = ARGV[i + 2]
            local pexpireat = (
                (tonumber(time[1]) + ttl) * 1000 + math.floor(tonumber(time[2]) / 1000)
            )
    
            redis.call('HSET', hash_name, key, value)
            redis.call('ZADD', zset_name, pexpireat, key)
    
        end
    end
    
    local max_pexpireat = tonumber(redis.call(
            'ZREVRANGEBYSCORE', zset_name, '+inf', '-inf', 'WITHSCORES', 'LIMIT', 0, 1
    )[2])
    
    redis.call('PEXPIREAT', hash_name, max_pexpireat)
    redis.call('PEXPIREAT', zset_name, max_pexpireat)
    """

    def __init__(self, hash_name: str, client: redis.Redis):
        self.hash_name = hash_name
        self.zset_name = f'{hash_name}.EXPIREAT'
        self.client = client
        self.lua_set_many = self.client.register_script(self.LUA_SET_MANY_SCRIPT)
        self.lua_get_many = self.client.register_script(self.LUA_GET_MANY_SCRIPT)
        self.lua_get_all = self.client.register_script(self.LUA_GET_ALL_SCRIPT)
        self.lua_vacuum = self.client.register_script(self.LUA_VACUUM_SCRIPT)
        self.lua_ttl = self.client.register_script(self.LUA_TTL_SCRIPT)

    def set(self, key: str, value: str, ttl: int) -> None:
        self.set_many(data=[(key, value, ttl)])

    def set_many(self, data: Sequence[Tuple[str, str, int]]) -> None:
        self.lua_set_many(
            keys=[self.hash_name, self.zset_name],
            args=list(itertools.chain.from_iterable(data)),
        )

    def get(self, key: str) -> Optional[str]:
        return self.get_many(keys=[key]).get(key)

    def get_many(self, keys: Sequence[str]) -> Dict[str, Optional[str]]:
        cache_data = self.lua_get_many(
            keys=[self.hash_name, self.zset_name], args=list(keys)
        )
        result = dict(zip(keys, cache_data))
        return result

    def get_all(self) -> Dict[str, str]:
        data = self.lua_get_all(keys=[self.hash_name, self.zset_name])
        return dict(zip(data[::2], data[1::2]))

    def delete(self, key: str) -> None:
        self.set(key=key, value='', ttl=-1)

    def vacuum(self, delete_count: int) -> None:
        self.lua_vacuum(keys=[self.hash_name, self.zset_name], args=[delete_count])

    def ttl(self, key) -> Optional[int]:
        return self.lua_ttl(keys=[self.zset_name], args=[key])


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
