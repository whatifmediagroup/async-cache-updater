import os


ONE_MINUTE = 60
ONE_HOUR = 60 * ONE_MINUTE
ONE_DAY = 24 * ONE_HOUR
ONE_WEEK = 7 * ONE_DAY


def env(name, default):
    return os.environ.get(name, default)


DEFAULT_SETTINGS = {
    'DEFAULT_CLIENT': None,
    'CACHE_KEY_PREFIX': env('CACHE_UPDATER_KEY_PREFIX', 'cache_updater'),
    'CACHE_INDEX_PREFIX': env('CACHE_UPDATER_INDEX_PREFIX', 'cache_index'),
    'CACHE_REFRESH_PREFIX': env('CACHE_UPDATER_REFRESH_PREFIX', 'cache_refresh_time'),
    'CACHE_UPDATED_PREFIX': env('CACHE_UPDATER_UPDATED_PREFIX', 'cache_updated_time'),
    'DEFAULT_TIMEZONE': env('CACHE_UPDATER_DEFAULT_TIMEZONE', 'US/Eastern'),
    'DEFAULT_TIMEOUT_TTL': env('CACHE_UPDATER_DEFAULT_TIMEOUT_TTL', ONE_HOUR),
    'DEFAULT_TIMEOUT_REFRESH': env('CACHE_UPDATER_DEFAULT_TIMEOUT_REFRESH', None),
    'DEFAULT_REFRESH_STRATEGY': env('CACHE_UPDATER_DEFAULT_REFRESH_STRATEGY', 'all'),
}


class CacheSettings:
    def __init__(self):
        for attr, val in DEFAULT_SETTINGS.items():
            setattr(self, attr, val)

    def setup(self, client, **kwargs):
        setattr(self, 'DEFAULT_CLIENT', client)
        for key, value in kwargs.items():
            setattr(self, key.lower(), value)


cache_settings = CacheSettings()
setup_client = cache_settings.setup
