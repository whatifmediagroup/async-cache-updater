#!/usr/bin/env python

"""Tests for `async_cache_updater` package."""
import asyncio
import time
from contextlib import contextmanager

import aioredis
import pytest
from tests import settings

from async_cache_updater import async_cache_updater, setup_client


pytestmark = pytest.mark.asyncio


@contextmanager
def timeit():
    start = time.time()
    yield
    end = time.time()
    print(f'{end - start:0.2f} seconds')


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
    with timeit():
        first = await basic_func('foo')
    with timeit():
        second = await basic_func('foo')
    assert first == second
