"""Caches the output of functions with time-based buckets


    >>> @async_cache_updater
    ... async def current_score(account_id):
    ...     return long_db_query('...')
    ...
    >>> await current_score(123)  # Cache miss
    21
    >>> await current_score(123)  # Cache hit
    21
    >>>
    >>> @async_cache_updater(bucket='monthly')
    ... async def avg_month_score(account_id, dt=None, tz=None):
    ...     month_start, month_end = get_monthly_range(dt, tz)
    ...     return long_db_query('...')
    ...
    >>> await avg_month_score(123)  # Current month - Cache miss
    47
    >>> await avg_month_score(123)  # Current month - Cache hit
    47
    >>> await avg_month_score(123, '2021-01-01')  # Cache miss
    33
    >>> await avg_month_score(123, '2021-01-01')  # Cache hit
    33
    >>> await avg_month_score(123, '2021-01-10')  # Cache hit
    33
    >>> await avg_month_score(123, '2021-01-20')  # Cache hit
    33
    >>> await avg_month_score(123, '2021-02-01')  # Cache miss
    25
"""

__author__ = """RevPoint Media"""
__email__ = 'tech@jangl.com'
__version__ = '0.1.0'

__all__ = ['async_cache_updater', 'cache_settings', 'setup_client']


from async_cache_updater.settings import cache_settings, setup_client
from async_cache_updater.decorators import async_cache_updater
