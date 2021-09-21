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
    if payload is not None:
        return pickle.loads(payload)


async def get(client, key, default=None):
    cache = get_cache_client(client)
    async with cache.client() as conn:
        result = await conn.get(key)
    if result is None:
        return default
    return _deserialize(result)


async def set(client, key, value, timeout=None):
    cache = get_cache_client(client)
    async with cache.client() as conn:
        await conn.set(key, _serialize(value), timeout)


async def delete(client, key):
    cache = get_cache_client(client)
    async with cache.client() as conn:
        await conn.delete(key)


async def get_many(client, keys):
    cache = get_cache_client(client)
    async with cache.client() as conn:
        values = await conn.mget(keys)
    return {
        key: value
        for key, value in zip(keys, map(_deserialize, values))
        if value is not None
    }


async def set_many(client, data, timeout=None):
    cache = get_cache_client(client)
    async with cache.client() as conn:
        await conn.mset({key: _serialize(val) for key, val in data.items()})


async def delete_many(client, keys):
    cache = get_cache_client(client)
    async with cache.client() as conn:
        await conn.delete(*keys)


async def update_index(client, cache_key, index_key, timeout):
    cache = get_cache_client(client)
    now = current_unix_time()
    async with cache.client() as conn:
        if timeout:
            await conn.zremrangebyscore(index_key, '-inf', now - timeout)
        await conn.zadd(index_key, {cache_key: now})


async def clear_index(client, index_key, before, after):
    cache = get_cache_client(client)
    async with cache.client() as conn:
        cache_keys = await conn.zrangebyscore(index_key, after, before)
        await delete_many(client, cache_keys)
        await conn.zremrangebyscore(index_key, after, before)
