import pickle
import typing

from aioredis import Redis

from async_cache_updater import cache_settings
from async_cache_updater.utils import current_unix_time


def get_cache_client(client):
    client = client or cache_settings.DEFAULT_CLIENT
    if client is None:
        raise RuntimeError(
            'Must run setup_client() before using async_cache_updater'
        )
    if not isinstance(client, Redis):
        raise ValueError(
            'Only aioredis can be used as cache backend'
        )
    return client


def _serialize(payload: typing.Any) -> bytes:
    return pickle.dumps(payload, protocol=-1)


def _deserialize(payload: bytes) -> typing.Any:
    return pickle.loads(payload)


async def get(client, key, default=None):
    client = get_cache_client(client)
    result = await client.get(key)
    if result is None:
        return default
    return _deserialize(result)


async def set(client, key, value, timeout=None):
    client = get_cache_client(client)
    await client.set(key, _serialize(value), timeout)


async def delete(client, key):
    client = get_cache_client(client)
    await client.delete(key)


async def get_many(client, keys):
    client = get_cache_client(client)
    values = await client.mget(keys)
    return list(map(_deserialize, values))


async def set_many(client, data, timeout=None):
    client = get_cache_client(client)
    await client.mset({key: _serialize(val) for key, val in data.items()})


async def delete_many(client, keys):
    client = get_cache_client(client)
    await client.delete(*keys)


async def update_index(client, cache_key, index_key, timeout):
    client = get_cache_client(client)
    now = current_unix_time()
    if timeout:
        await client.zremrangebyscore(index_key, '-inf', now - timeout)
    await client.zadd(index_key, {cache_key: now})


async def clear_index(client, index_key, before, after):
    client = get_cache_client(client)
    cache_keys = await client.zrangebyscore(index_key, after, before)
    await delete_many(client, cache_keys)
    await client.zremrangebyscore(index_key, after, before)
