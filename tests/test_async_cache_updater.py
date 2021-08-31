#!/usr/bin/env python

"""Tests for `async_cache_updater` package."""
import asyncio
import time

import aioredis
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
    redis_client = await aioredis.Redis.from_url(settings.REDIS_CACHE_URL)
    await redis_client.flushdb()
    setup_client(redis_client)
    yield redis_client
    await redis_client.flushdb()
    await redis_client.close()


@async_cache_updater
async def basic_func(arg):
    await asyncio.sleep(1.0)
    return arg


async def test_basic_func(redis_client):
    first = await basic_func('foo')
    second = await basic_func('foo')
    assert first == second


async def test_basic_func_timer(redis_client):
    first = await time_async(basic_func, 'foo')
    second = await time_async(basic_func, 'foo')
    assert first > 1.0
    assert second < 1.0
