import os
import typing

ONE_MINUTE = 60
ONE_HOUR = 60 * ONE_MINUTE
ONE_DAY = 24 * ONE_HOUR
ONE_WEEK = 7 * ONE_DAY


def env(name, default):
    return os.environ.get(f'CACHE_UPDATER_{name.upper()}', default)


def bool_value(value) -> bool:
    valid = ('true', 't', 'yes', 'y', '1')
    return str(value).strip().lower() in valid


def int_value(value) -> typing.Optional[int]:
    if value in (None, ''):
        return
    return int(value)


DEFAULT_SETTINGS = {
    'KEY_PREFIX': 'cache_updater',
    'INDEX_PREFIX': 'cache_index',
    'REFRESH_PREFIX': 'cache_refresh_time',
    'UPDATED_PREFIX': 'cache_updated_time',
    'DEFAULT_TIMEZONE': 'US/Eastern',
    'DEFAULT_TIMEOUT_TTL': ONE_HOUR,
    'DEFAULT_TIMEOUT_REFRESH': None,
    'DEFAULT_REFRESH_STRATEGY': 'all',
    'DISABLED': False,
}

SETTINGS_MAPPING = {
    'DEFAULT_TIMEOUT_TTL': int_value,
    'DEFAULT_TIMEOUT_REFRESH': int_value,
    'DISABLED': bool_value,
}


class CacheSettings:
    DEFAULT_CLIENT = None

    def __init__(self):
        for name, default in DEFAULT_SETTINGS.items():
            value = env(name, default)
            self.set_setting(name, value)

    def setup(self, client, **kwargs):
        self.DEFAULT_CLIENT = client
        for name, value in kwargs.items():
            name = name.upper().replace('CACHE_UPDATER_', '')
            if name in DEFAULT_SETTINGS:
                self.set_setting(name, value)

    def set_setting(self, name, value):
        _type = SETTINGS_MAPPING.get(name, str)
        setattr(self, name, _type(value))


cache_settings = CacheSettings()
setup_client = cache_settings.setup
