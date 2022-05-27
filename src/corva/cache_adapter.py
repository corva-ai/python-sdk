import itertools
from datetime import timedelta
from typing import (
    Dict,
    List,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Union,
    cast,
    overload,
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
    # Deletes M expired keys from the hash.
    #
    # Complexity: O(log(N) + M) + O(M) + O(M * log(N)) where N is the size of hash
    # and M is the number of elements deleted. For constant M (e.g., 3) the complexity
    # is O(log(N)).
    #
    # KEYS:
    #      hash_name.
    #      zset_name.
    #
    # ARGV:
    #      delete_count.
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

    # Gets either all or only requested keys and values from hash.
    #
    # Gets all keys if no keys specified in ARGV.
    #
    # Complexity: O(N) where N is the number of requested keys.
    #
    # KEYS:
    #     hash_name.
    #     zset_name.
    #
    # Optional ARGV:
    #     key1.
    #     key2.
    #     ...
    #
    # Returns: list of names and values for non-expired keys.
    LUA_GET_SCRIPT = """
    local hash_name = KEYS[1]
    local zset_name = KEYS[2]
    local time = redis.call('TIME')
    local pnow = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

    local keys = ARGV
    if next(ARGV) == nil then
        keys = redis.call('HKEYS', hash_name)
    end

    if not next(keys) then
        return {}
    end

    local hash = redis.call('HMGET', hash_name, unpack(keys))

    local result = {}

    for i, key in ipairs(keys) do
        local pexpireat = redis.call('ZSCORE', zset_name, key)

        if not pexpireat or pnow < tonumber(pexpireat) then
            table.insert(result, key)
            table.insert(result, hash[i])
        end
    end

    return result
    """

    # Inserts list of keys-value-expiration tuples into the hash.
    #
    # Complexity: O(N * log(M)), where N is the number of inserted elements and
    # M is the hash size.
    #
    # 1. If hash does not exist, it will automatically create one.
    # 2. If the field already exists, its value and ttl will be overwritten.
    # 3. Hash and zset ttl is always set to the biggest field's ttl.
    # 4. When the field expires it may be deleted by:
    #    - Manually invoking `vacuum` script.
    #    - Redis automatically deleting expired hash (see note #3).
    #
    # KEYS:
    #     hash_name.
    #     zset_name.
    #
    # ARGV:
    #     key.
    #     value.
    #     ttl.
    #     ...
    #
    # Returns: nil.
    LUA_SET_SCRIPT = """
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

    # Deletes all data from hash and zset.
    #
    # Complexity: O(N) where N is the hash size.
    #
    # KEYS:
    #     hash_name.
    #     zset_name.
    #
    # Returns: nil.
    LUA_DELETE_ALL_SCRIPT = """
    local hash_name = KEYS[1]
    local zset_name = KEYS[2]

    redis.call('DEL', hash_name, zset_name)
    """

    def __init__(self, hash_name: str, client: redis.Redis, use_lua_52: bool = False):
        self.hash_name = hash_name
        self.zset_name = f'{hash_name}.EXPIREAT'
        self.client = client
        self.lua_set_many = self.client.register_script(self.LUA_SET_SCRIPT)

        lua_get_script = self.LUA_GET_SCRIPT
        if use_lua_52:
            # Hack for tests to work with fakeredis, as it uses Lua version > 5.1.
            # In Lua 5.1, unpack was a global, but in 5.2 it's been moved to
            # table.unpack.
            lua_get_script = self.LUA_GET_SCRIPT.replace('unpack', 'table.unpack')
        self.lua_get = self.client.register_script(lua_get_script)

        self.lua_vacuum = self.client.register_script(self.LUA_VACUUM_SCRIPT)
        self.lua_delete_all = self.client.register_script(self.LUA_DELETE_ALL_SCRIPT)

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
        data = self.lua_get(keys=[self.hash_name, self.zset_name], args=list(keys))

        data = dict(zip(data[::2], data[1::2]))

        for missing_key in set(keys) - set(data):
            data[missing_key] = None

        return data

    def get_all(self) -> Dict[str, str]:
        return cast(Dict[str, str], self.get_many(keys=[]))

    def delete(self, key: str) -> None:
        self.delete_many(keys=[key])

    def delete_many(self, keys: Sequence[str]) -> None:
        self.set_many(data=[(key, '', -1) for key in keys])

    def delete_all(self) -> None:
        self.lua_delete_all(keys=[self.hash_name, self.zset_name])

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

    @overload
    def hset(
        self,
        name: Optional[str] = ...,
        key: str = ...,
        value: REDIS_STORED_VALUE_TYPE = ...,
        mapping: None = ...,
        expiry: Union[int, timedelta, None] = ...,
    ) -> int:
        ...

    @overload
    def hset(
        self,
        name: Optional[str] = ...,
        key: None = ...,
        value: None = ...,
        mapping: Dict[str, REDIS_STORED_VALUE_TYPE] = ...,
        expiry: Union[int, timedelta, None] = ...,
    ) -> int:
        ...

    def hset(
        self,
        name=None,
        key=None,
        value=None,
        mapping=None,
        expiry=DEFAULT_EXPIRY,
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

        delete_names = names or [self.default_name]
        return self.client.delete(*delete_names)

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

        exists_names = names or [self.default_name]
        return self.client.exists(*exists_names)
