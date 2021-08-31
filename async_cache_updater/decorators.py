import asyncio
import functools
import inspect
import logging
import re

import aioredis
import pytz

from async_cache_updater import cache, cache_settings
from async_cache_updater.buckets import generate_bucket_name, get_bucket
from async_cache_updater.timezone import tz_now
from async_cache_updater.types import (
    BucketTypes,
    CachedFunctionType,
    CachedDecoratorType,
    DefaultDTMethodType,
    TZLookupMethodType,
)
from async_cache_updater.utils import (
    current_unix_time,
    find_bucket_ranges,
    force_async,
    hash_key,
    get_bucket_range,
    latest_bucket_ranges,
    parse_timestamp,
)

_redis_exceptions = (
    aioredis.RedisError,
    asyncio.TimeoutError,
)

logger = logging.getLogger(__name__)
lookup_name_re = re.compile(r'^[-.\w]+$')
_empty = object()


def async_cache_updater(
    fn: CachedFunctionType = None,
    bucket: BucketTypes = None,
    client: aioredis.Redis = None,
    default_dt: DefaultDTMethodType = tz_now,
    lookup_name: str = None,
    refresh_strategy: str = cache_settings.DEFAULT_REFRESH_STRATEGY,
    timeout_refresh: int = cache_settings.DEFAULT_TIMEOUT_REFRESH,
    timeout_ttl: int = cache_settings.DEFAULT_TIMEOUT_TTL,
    timestamp_name: str = 'dt',
    tz_name: str = 'tz',
    tz_lookup: TZLookupMethodType = None,
) -> CachedDecoratorType:
    """Decorator that caches the output of a function.

    Generates a cached function that will memoize based on the called
    arguments. Can also use time-based buckets for cache lookups.

    Bucket can be:
    - None (Buckets are disabled)
    - Any of: 'monthly', 'weekly', 'daily', 'hourly'
    - A function that is passed a datetime and timezone and returns a str


    :param fn: This function is passed to decorator with all cache defaults
    :param bucket: Time-based bucket used for the cache key
    :param client: Redis cache instance to be used
    :param default_dt: Function that is called when timestamp is None
    :param lookup_name: Name of the function saved to the cache
    :param refresh_strategy: Strategy that determines if refresh should
        run on `all` buckets or only `latest` bucket
    :param timeout_refresh: Number of seconds until a cache hit will trigger
        a background cache update
    :param timeout_ttl: Number of seconds until value is deleted from cache
    :param timestamp_name: Field name for the timestamp that is passed to
        the cached function
    :param tz_name: Field name for the timezone that is passed to the
        cached function
    :param tz_lookup: Function that is used to lookup the timezone used for
        the cache bucket

    :raise: ValueError if cached function is missing arguments
    :return: Decorator that generates a cached function

    """

    def _decorator(func: CachedFunctionType) -> CachedFunctionType:
        @functools.wraps(func)
        async def cached_func(*args, **kwargs):
            force_cache = kwargs.pop('force_cache', False)
            force_refresh = kwargs.pop('force_refresh', False)
            cache_ttl = kwargs.pop('cache_ttl', timeout_ttl)

            call_args = await get_call_args(*args, **kwargs)
            cache_key = get_cache_key(call_args)

            if not force_cache:
                result = await cache.get(client, cache_key)
                if result is not None:
                    logger.info('CACHE HIT: {}'.format(cache_key))
                    if force_refresh or await should_refresh(
                        call_args, cache_key,
                    ):
                        run_cache_refresh(call_args, cache_key, cache_ttl)
                    return result
                logger.info('CACHE MISS: {}'.format(cache_key))

            return await run_and_cache(call_args, cache_key, cache_ttl)

        async def get_timeseries(*args, **kwargs):
            start_dt = kwargs.pop('start_dt', None)
            end_dt = kwargs.pop('end_dt', None)
            assert start_dt and end_dt, 'Requires "start_dt" and "end_dt"'

            call_args = await get_call_args(*args, **kwargs)
            tz = call_args[tz_name]

            timestamps = [
                start for (start, end) in
                find_bucket_ranges(bucket, start_dt, end_dt, tz)
            ]
            return await retrieve_many_buckets(call_args, timestamps)

        async def get_latest_timeseries(*args, **kwargs):
            num_buckets = kwargs.pop('num_buckets', None)
            assert num_buckets is not None, 'Requires "num_buckets"'

            call_args = await get_call_args(*args, **kwargs)
            dt = call_args[timestamp_name]
            tz = call_args[tz_name]

            timestamps = [
                start for (start, end) in
                latest_bucket_ranges(bucket, dt, tz, num_buckets)
            ]
            return await retrieve_many_buckets(call_args, timestamps)

        async def retrieve_many_buckets(call_args, timestamps):
            series = {}
            for timestamp in timestamps:
                call_args[timestamp_name] = timestamp
                cache_key = get_cache_key(call_args)
                series[cache_key] = timestamp
            lookup_keys = list(series)
            results = await cache.get_many(client, lookup_keys)
            found = [key for key in lookup_keys if key in results]
            missing = [key for key in lookup_keys if key not in results]

            for cache_key in missing:
                bucket_args = call_args.copy()
                bucket_args[timestamp_name] = series[cache_key]
                results[cache_key] = await run_and_cache(
                    bucket_args, cache_key, timeout_ttl,
                )

            refresh_info = await get_refresh_info(found)
            for cache_key, refresh_at, updated_at in refresh_info:
                bucket_args = call_args.copy()
                bucket_args[timestamp_name] = series[cache_key]
                if await should_refresh(
                    bucket_args, cache_key, refresh_at, updated_at,
                ):
                    run_cache_refresh(bucket_args, cache_key, timeout_ttl)

            return [
                (timestamp, results[cache_key])
                for cache_key, timestamp in series.items()
            ]

        async def get_call_args(*args, **kwargs):
            [kwargs.pop(kw, None) for kw in kwargs if kw not in func_args]

            call_args = inspect.getcallargs(func, *args, **kwargs)
            # If bucket is None, ignore timestamp and timezone
            if bucket is None:
                return call_args

            timezone = call_args[tz_name]
            if timezone is None:
                timezone = cache_settings.DEFAULT_TIMEZONE
                # if tz_lookup is defined, call it with any common args
                if tz_lookup is not None:
                    tz_lookup_kwargs = {
                        arg: call_args[arg] for arg in tz_lookup_args
                    }
                    timezone = await force_async(tz_lookup, **tz_lookup_kwargs)

            # convert strings to pytz timezones
            if isinstance(timezone, str):
                timezone = pytz.timezone(timezone)

            call_args[tz_name] = timezone

            # Timestamp can accept:
            # None, a function, date string, datetime string,
            # date object, datetime object
            timestamp = call_args[timestamp_name]
            if timestamp is None:
                timestamp = default_dt
            if callable(timestamp):
                timestamp = timestamp()

            call_args[timestamp_name] = parse_timestamp(timestamp, timezone)

            return call_args

        def get_cache_key(call_args):
            arglist = [
                str(call_args[arg])
                for arg in func_args
                if arg not in (timestamp_name, tz_name)
            ]
            cache_args = [
                cache_settings.KEY_PREFIX,
                func_module,
                func_name,
                ':'.join(arglist),
            ]
            if bucket is not None:
                bucket_name = generate_bucket_name(
                    bucket,
                    call_args[timestamp_name],
                    call_args[tz_name],
                )
                cache_args.append(bucket_name)

            cache_key = ':'.join(cache_args)
            logger.debug(cache_key)
            return hash_key(cache_key)

        def get_index_key():
            index_key = '{}:{}:{}'.format(
                cache_settings.INDEX_PREFIX,
                func_module,
                func_name,
            )
            logger.debug(index_key)
            return index_key

        def get_updated_key(cache_key):
            return '{}:{}'.format(
                cache_settings.UPDATED_PREFIX,
                cache_key,
            )

        def get_refresh_key(cache_key):
            return '{}:{}'.format(
                cache_settings.REFRESH_PREFIX,
                cache_key,
            )

        async def run_and_cache(call_args, cache_key, cache_ttl):
            output = await force_async(func, **call_args)
            await save_to_cache(output, cache_key, cache_ttl)
            return output

        async def save_to_cache(output, cache_key, cache_ttl):
            try:
                cache_data = {
                    cache_key: output,
                    **set_refresh_at(cache_key),
                    **set_updated_at(cache_key),
                }
                await cache.set_many(client, cache_data, cache_ttl)
                await update_cache_index(cache_key, cache_ttl)
                logger.info('SAVED TO CACHE: {}'.format(cache_key))
            except _redis_exceptions:
                logger.info('CACHE WRITE ERROR: {}'.format(cache_key))

        async def get_refresh_at(cache_key):
            refresh_key = get_refresh_key(cache_key)
            return await cache.get(client, refresh_key)

        async def get_updated_at(cache_key):
            updated_key = get_updated_key(cache_key)
            return await cache.get(client, updated_key)

        async def get_refresh_info(cache_keys):
            refresh_keys = [get_refresh_key(key) for key in cache_keys]
            updated_keys = [get_updated_key(key) for key in cache_keys]
            results = await cache.get_many(client, refresh_keys + updated_keys)
            refresh_times = [results.get(key) for key in refresh_keys]
            updated_times = [results.get(key) for key in updated_keys]
            return zip(cache_keys, refresh_times, updated_times)

        async def update_cache_index(cache_key, cache_ttl):
            index_key = get_index_key()
            await cache.update_index(client, cache_key, index_key, cache_ttl)

        def set_refresh_at(cache_key):
            if timeout_refresh is None:
                return {}
            refresh_key = get_refresh_key(cache_key)
            time = current_unix_time()
            refresh_at = time + timeout_refresh
            return {refresh_key: refresh_at}

        def set_updated_at(cache_key):
            updated_key = get_updated_key(cache_key)
            return {updated_key: tz_now()}

        async def should_refresh(
            call_args, cache_key, refresh_at=_empty, updated_at=_empty
        ):
            if timeout_refresh is None:
                return False
            if refresh_at is _empty:
                refresh_at = await get_refresh_at(cache_key)
            if refresh_at is not None and refresh_at > current_unix_time():
                return False
            if refresh_strategy == 'latest':
                dt, tz = call_args[timestamp_name], call_args[tz_name]
                current_bucket = generate_bucket_name(bucket, dt, tz)
                latest_bucket = generate_bucket_name(bucket, default_dt(), tz)
                if current_bucket != latest_bucket:
                    bucket_start, bucket_end = get_bucket_range(bucket, dt, tz)
                    if updated_at is _empty:
                        updated_at = await get_updated_at(cache_key)
                    if bucket_start > tz_now():
                        return False
                    if updated_at is not None and updated_at > bucket_end:
                        return False
            return True

        def run_cache_refresh(call_args, cache_key, cache_ttl):
            logger.info('REFRESHING CACHE: {}'.format(cache_key))
            asyncio.create_task(
                run_and_cache(call_args, cache_key, cache_ttl)
            )

        async def clear_cache(before='+inf', after='-inf'):
            index_key = get_index_key()
            await cache.clear_index(client, index_key, before, after)

        def get_func_def():
            lines, line_no = inspect.getsourcelines(func)
            for line in lines:
                if line.strip().startswith('def '):
                    return line_no, line.strip()
            return line_no, lines[0].strip()

        def check_valid_func():
            if hasattr(func, '_original_func'):
                raise AttributeError(
                    'This method is already has async_cache_updater applied'
                )
            if lookup_name and lookup_name_re.match(lookup_name) is None:
                raise ValueError('lookup_name contains invalid characters')

            # Remaining checks require bucket
            if bucket is None:
                return
            err = 'Cached method requires {} argument "{}=None". (L#{} `{}`)'
            if timestamp_name not in func_args:
                raise ValueError(
                    err.format('timestamp', timestamp_name, *get_func_def())
                )
            if tz_name not in func_args:
                raise ValueError(
                    err.format('timezone', tz_name, *get_func_def())
                )
            if tz_lookup is not None:
                for arg in tz_lookup_args:
                    if arg not in func_args:
                        raise ValueError(
                            'tz_lookup requires that this function has the '
                            'argument "{}"'.format(arg)
                        )

        func_args = inspect.getfullargspec(func)[0]
        func_name = lookup_name or func.__name__
        func_module = func.__module__

        check_valid_func()
        cached_func._original_func = func
        for name, val in locals().items():
            if 'func' not in name:
                setattr(cached_func, name, val)

        return cached_func

    bucket = get_bucket(bucket)
    tz_lookup_args = []
    tz_lookup_func = getattr(tz_lookup, '_original_func', tz_lookup)
    if tz_lookup_func is not None:
        tz_lookup_args = inspect.getfullargspec(tz_lookup_func)[0]

    if fn is None:
        return _decorator
    return _decorator(fn)
