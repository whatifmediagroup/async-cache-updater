import asyncio
import calendar
import hashlib
from datetime import date, datetime, timedelta

from asgiref.sync import sync_to_async
from dateutil.relativedelta import relativedelta

from async_cache_updater.dateparse import parse_date, parse_datetime
from async_cache_updater.timezone import (
    is_aware,
    localtime,
    make_aware,
    tz_now,
    utc,
)


def force_async(func, *args, **kwargs):
    if not asyncio.iscoroutinefunction(func):
        func = sync_to_async(func, thread_sensitive=True)
    return func(*args, **kwargs)


def hash_key(value):
    return hashlib.sha1(value.encode('utf8')).hexdigest()


# Timestamp utils

def parse_timestamp(timestamp, timezone=utc):
    if isinstance(timestamp, str):
        timestamp = parse_date(timestamp) or parse_datetime(timestamp)

    # Everything up to here must produce a date or datetime object
    if not isinstance(timestamp, date):
        raise ValueError('Value must be a timestamp')

    # datetime is subclass of date, so the previous check allows both.
    # convert any date to datetime and fix any tz issues
    if not isinstance(timestamp, datetime):
        timestamp = date_to_datetime(timestamp, timezone)
    if not is_aware(timestamp):
        timestamp = make_aware(timestamp, timezone)
    return timestamp


def datetime_to_date(dt, tz):
    return localtime(dt, tz).date()


def date_to_datetime(dt, tz):
    day_start = datetime.combine(dt, datetime.min.time())
    return localtime(make_aware(day_start, tz), utc)


def local_strftime(dt, tz, format_str):
    return localtime(dt, tz).strftime(format_str)


def current_unix_time():
    dt = tz_now()
    secs = calendar.timegm(dt.utctimetuple())
    us = (dt.microsecond / 1e6)
    return secs + us


def datetime_from_unix(val):
    return datetime.fromtimestamp(val, utc)


# Time bucket ranges

_us = timedelta(microseconds=1)


def get_hourly_range(dt, tz):
    start = localtime(dt, tz).replace(minute=0, second=0, microsecond=0)
    end = start + timedelta(hours=1)
    return localtime(start, utc), localtime(end, utc) - _us


def get_daily_range(dt, tz):
    start = datetime_to_date(dt, tz)
    end = start + timedelta(days=1)
    return date_to_datetime(start, tz), date_to_datetime(end, tz) - _us


def get_weekly_range(dt, tz):
    start = datetime_to_date(dt, tz)
    while start.weekday():
        start -= timedelta(days=1)
    end = start + timedelta(days=7)
    return date_to_datetime(start, tz), date_to_datetime(end, tz) - _us


def get_monthly_range(dt, tz):
    today = datetime_to_date(dt, tz)
    start = today.replace(day=1)
    end = start + relativedelta(months=1)
    return date_to_datetime(start, tz), date_to_datetime(end, tz) - _us


def get_bucket_range(bucket, dt, tz):
    dt = parse_timestamp(dt, tz)
    start = find_bucket_start(bucket, dt, tz)
    step = find_bucket_step(bucket, start, tz)
    end = tz_delta_add(start, tz, step) - _us
    return start, end


def find_bucket_ranges(bucket, start, end, tz):
    start_dt = parse_timestamp(start, tz)
    end_dt = parse_timestamp(end, tz)
    assert start_dt <= end_dt

    bucket_start = find_bucket_start(bucket, start_dt, tz)
    step = find_bucket_step(bucket, bucket_start, tz)
    bucket_end = tz_delta_add(bucket_start, tz, step) - _us
    buckets = [(bucket_start, bucket_end)]
    while bucket_end < end_dt:
        bucket_start = tz_delta_add(bucket_start, tz, step)
        bucket_end = tz_delta_add(bucket_start, tz, step) - _us
        buckets.append((bucket_start, bucket_end))
    return buckets


def find_bucket_names(bucket, start, end, tz):
    ranges = find_bucket_ranges(bucket, start, end, tz)
    return [bucket(start, tz) for start, end in ranges]


def latest_bucket_ranges(bucket, dt, tz, num_buckets=1):
    assert num_buckets > 0
    dt = parse_timestamp(dt, tz)

    buckets = []
    start = find_bucket_start(bucket, dt, tz)
    step = find_bucket_step(bucket, start, tz)
    end = tz_delta_add(start, tz, step) - _us
    for _ in range(num_buckets):
        buckets.append((start, end))
        end = start - _us
        start = tz_delta_add(start, tz, -step)
    return list(reversed(buckets))


def latest_bucket_names(bucket, dt, tz, num_buckets=1):
    ranges = latest_bucket_ranges(bucket, dt, tz, num_buckets)
    return [bucket(start, tz) for start, end in ranges]


def check_bucket_delta(bucket, dt, tz, delta):
    # Confirms that the delta changes the bucket by applying it twice
    start_bucket = bucket(dt, tz)
    previous_bucket = bucket(tz_delta_add(dt, tz, delta), tz)
    if previous_bucket != start_bucket:
        if bucket(tz_delta_add(dt, tz, (delta * 2)), tz) != previous_bucket:
            return True
    return False


def find_bucket_edge(bucket, dt, tz, delta, check_range=1):
    for i in range(check_range):
        if check_bucket_delta(bucket, dt, tz, delta * (i + 1)):
            edge_dt = dt
            edge_bucket = bucket(edge_dt, tz)
            while bucket(tz_delta_add(edge_dt, tz, delta), tz) == edge_bucket:
                edge_dt = tz_delta_add(edge_dt, tz, delta)
            return localtime(edge_dt, utc)


def find_bucket_start(bucket, dt, tz):
    # Look for beginning of bucket by seeking time towards the past.
    start_dt = localtime(dt, tz)

    # Search for pattern up to 30 seconds
    start_second = start_dt.replace(microsecond=0)
    delta_second = timedelta(seconds=-1)
    first_second = find_bucket_edge(bucket, start_second, tz, delta_second, 30)
    if first_second:
        return first_second

    # Search for pattern up to 30 minutes
    start_minute = start_second.replace(second=0)
    delta_minute = timedelta(minutes=-1)
    first_minute = find_bucket_edge(bucket, start_minute, tz, delta_minute, 30)
    if first_minute:
        return first_minute

    # Search for pattern up to 12 hours
    start_hour = start_minute.replace(minute=0)
    delta_hour = timedelta(hours=-1)
    first_hour = find_bucket_edge(bucket, start_hour, tz, delta_hour, 12)
    if first_hour:
        return first_hour

    # Search for pattern up to 15 days
    start_day = start_hour.replace(hour=0)
    delta_day = relativedelta(days=-1)
    first_day = find_bucket_edge(bucket, start_day, tz, delta_day, 15)
    if first_day:
        return first_day

    # Search for pattern up to 6 months
    start_month = start_day.replace(day=1)
    delta_month = relativedelta(months=-1)
    first_month = find_bucket_edge(bucket, start_month, tz, delta_month, 6)
    if first_month:
        return first_month

    # Search for pattern up to 10 years
    start_year = start_month.replace(month=1)
    delta_year = relativedelta(years=-1)
    first_year = find_bucket_edge(bucket, start_year, tz, delta_year, 10)
    if first_year:
        return first_year

    raise ValueError(
        'Could not detect change in time with bucket method "{}"'.format(
            bucket.__name__
        )
    )


def find_bucket_step(bucket, dt, tz):
    start_dt = localtime(dt, tz)

    # Search for pattern up to 30 seconds
    start_second = start_dt.replace(microsecond=0)
    for i in range(30):
        delta_second = timedelta(seconds=i + 1)
        if check_bucket_delta(bucket, start_second, tz, delta_second):
            return delta_second

    # Search for pattern up to 30 minutes
    start_minute = start_second.replace(second=0)
    for i in range(30):
        delta_minute = timedelta(minutes=i + 1)
        if check_bucket_delta(bucket, start_minute, tz, delta_minute):
            return delta_minute

    # Search for pattern up to 12 hours
    start_hour = start_minute.replace(minute=0)
    for i in range(12):
        delta_hour = timedelta(hours=i + 1)
        if check_bucket_delta(bucket, start_hour, tz, delta_hour):
            return delta_hour

    # Search for pattern up to 15 days
    start_day = start_hour.replace(hour=0)
    for i in range(15):
        delta_day = relativedelta(days=i + 1)
        if check_bucket_delta(bucket, start_day, tz, delta_day):
            return delta_day

    # Search for pattern up to 6 months
    start_month = start_day.replace(day=1)
    for i in range(6):
        delta_month = relativedelta(months=i + 1)
        if check_bucket_delta(bucket, start_month, tz, delta_month):
            return delta_month

    # Search for pattern up to 10 years
    start_year = start_month.replace(month=1)
    for i in range(10):
        delta_year = relativedelta(years=i + 1)
        if check_bucket_delta(bucket, start_year, tz, delta_year):
            return delta_year

    raise ValueError(
        'Could not detect change in time with bucket method "{}"'.format(
            bucket.__name__
        )
    )


def delta_gt_1_day(delta):
    if isinstance(delta, timedelta):
        delta_seconds = abs(int(delta.total_seconds()))
        one_day_seconds = int(timedelta(days=1).total_seconds())
        if delta_seconds >= one_day_seconds:
            if delta_seconds % one_day_seconds != 0:
                raise ValueError(
                    'Cannot have time when using a delta > 1 day'
                )
            return True

    if isinstance(delta, relativedelta):
        if any(
            (
                delta.years,
                delta.months,
                delta.days,
                delta.leapdays,
                delta.year is not None,
                delta.month is not None,
                delta.day is not None,
                delta.weekday is not None,
            )
        ):
            if delta._has_time:
                raise ValueError(
                    'Cannot have time when using a delta > 1 day'
                )
            return True
    return False


def tz_delta_add(dt, tz, delta):
    if delta_gt_1_day(delta):
        dt = datetime_to_date(dt, tz)
    dt = dt + delta
    if not isinstance(dt, datetime):
        dt = date_to_datetime(dt, tz)
    return dt
