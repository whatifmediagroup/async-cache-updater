import datetime
from typing import Optional

import pytz

from async_cache_updater import utils
from async_cache_updater.types import BucketMethodType, BucketTypes


def hourly(dt, tz):
    start, end = utils.get_hourly_range(dt, tz)
    return utils.local_strftime(start, tz, '%Y-%m-%dT%H')


def daily(dt, tz):
    start, end = utils.get_daily_range(dt, tz)
    return utils.local_strftime(start, tz, '%Y-%m-%d')


def weekly(dt, tz):
    start, end = utils.get_weekly_range(dt, tz)
    return utils.local_strftime(start, tz, '%Yw%W')


def monthly(dt, tz):
    start, end = utils.get_monthly_range(dt, tz)
    return utils.local_strftime(start, tz, '%Y-%m')


BUCKET_LOOKUPS = {
    'hourly': hourly,
    'daily': daily,
    'weekly': weekly,
    'monthly': monthly,
}


def get_bucket(bucket: BucketTypes) -> Optional[BucketMethodType]:
    if bucket is None or callable(bucket):
        return bucket
    return BUCKET_LOOKUPS[bucket]


def generate_bucket_name(
    bucket: BucketTypes, dt: datetime.datetime, tz: pytz.timezone
) -> str:
    bucket_lookup = get_bucket(bucket)
    return bucket_lookup(dt, tz)
