#!/usr/bin/env python

"""Tests for `async_cache_updater` package."""
import asyncio
import time

import redis.asyncio as redis
import pytest
from tests import settings

from async_cache_updater import async_cache_updater, setup_client


pytestmark = pytest.mark.asyncio


async def time_async(func, *args, **kwargs):
    start = time.time()
    await func(*args, **kwargs)
    end = time.time()
    return end - start


@pytest.fixture
async def redis_client(event_loop):
    redis_client = redis.Redis.from_url(settings.REDIS_CACHE_URL)
    await redis_client.flushdb()
    setup_client(redis_client)
    yield redis_client
    await redis_client.flushdb()
    await redis_client.close()


@async_cache_updater
async def basic_func(arg):
    await asyncio.sleep(1.0)
    return arg


@async_cache_updater(ignore_args=['ignore_me'])
async def func_with_ignored_args(arg, ignore_me):
    await asyncio.sleep(0.5)
    return f"{arg}_{ignore_me}"


@async_cache_updater(ignore_args=['session_id', 'request_id'])
async def func_with_multiple_ignored_args(data, session_id, request_id, cache_key):
    await asyncio.sleep(0.5)
    return f"{data}_{cache_key}"


async def test_basic_func(redis_client):
    first = await basic_func('foo')
    second = await basic_func('foo')
    assert first == second


async def test_basic_func_timer(redis_client):
    first = await time_async(basic_func, 'foo')
    second = await time_async(basic_func, 'foo')
    assert first > 1.0
    assert second < 1.0


async def test_ignore_args_single(redis_client):
    # These should return the same cached result despite different ignore_me values
    first = await func_with_ignored_args('test', 'session1')
    second = await func_with_ignored_args('test', 'session2')
    
    # Both should return the same cached result (from first call)
    assert first == second == 'test_session1'


async def test_ignore_args_single_timer(redis_client):
    # First call should be slow (not cached)
    first_time = await time_async(func_with_ignored_args, 'test', 'session1')
    # Second call should be fast (cached) even with different ignored arg
    second_time = await time_async(func_with_ignored_args, 'test', 'session2')
    
    assert first_time > 0.4  # Should take at least 0.5s
    assert second_time < 0.4  # Should be much faster (cached)


async def test_ignore_args_multiple(redis_client):
    # These should return the same cached result despite different session_id and request_id
    first = await func_with_multiple_ignored_args('data1', 'sess1', 'req1', 'key1')
    second = await func_with_multiple_ignored_args('data1', 'sess2', 'req2', 'key1')
    
    # Both should return the same cached result (from first call)
    assert first == second == 'data1_key1'


async def test_ignore_args_different_cache_keys(redis_client):
    # These should NOT return the same result because cache_key is not ignored
    first = await func_with_multiple_ignored_args('data1', 'sess1', 'req1', 'key1')
    second = await func_with_multiple_ignored_args('data1', 'sess2', 'req2', 'key2')
    
    # Should return different results because cache_key differs
    assert first == 'data1_key1'
    assert second == 'data1_key2'
    assert first != second
