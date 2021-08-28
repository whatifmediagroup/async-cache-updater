from datetime import datetime, timedelta, tzinfo

try:
    import pytz
except ImportError:
    pytz = None


ZERO = timedelta(0)


class FixedOffset(tzinfo):
    """
    Fixed offset in minutes east from UTC. Taken from Python's docs.
    Kept as close as possible to the reference version. __init__ was changed
    to make its arguments optional, according to Python's requirement that
    tzinfo subclasses can be instantiated without arguments.
    """

    def __init__(self, offset=None, name=None):
        if offset is not None:
            self.__offset = timedelta(minutes=offset)
        if name is not None:
            self.__name = name

    def utcoffset(self, dt):
        return self.__offset

    def tzname(self, dt):
        return self.__name

    def dst(self, dt):
        return ZERO


class UTC(tzinfo):
    """
    UTC implementation taken from Python's docs.

    Used only when pytz isn't available.
    """

    def __repr__(self):
        return "<UTC>"

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO


utc = pytz.utc if pytz else UTC()


def get_fixed_timezone(offset):
    """Return a tzinfo instance with a fixed offset from UTC."""
    if isinstance(offset, timedelta):
        offset = offset.seconds // 60
    sign = '-' if offset < 0 else '+'
    hhmm = '%02d%02d' % divmod(abs(offset), 60)
    name = sign + hhmm
    return FixedOffset(offset, name)


def is_aware(value):
    return (
        value.tzinfo is not None and value.tzinfo.utcoffset(value) is not None
    )


def is_naive(value):
    return value.tzinfo is None or value.tzinfo.utcoffset(value) is None


def make_aware(value: datetime, timezone: tzinfo = None) -> datetime:
    """
    Makes a naive datetime.datetime in a given time zone aware.
    """
    if timezone is None:
        timezone = utc
    if hasattr(timezone, 'localize'):
        # This method is available for pytz time zones.
        return timezone.localize(value, is_dst=None)
    else:
        # Check that we won't overwrite the timezone of an aware datetime.
        if is_aware(value):
            raise ValueError(
                "make_aware expects a naive datetime, got %s" % value)
        # This may be wrong around DST changes!
        return value.replace(tzinfo=timezone)


def make_naive(value: datetime, timezone: tzinfo = None) -> datetime:
    """
    Makes an aware datetime.datetime naive in a given time zone.
    """
    if timezone is None:
        timezone = utc
    # If `value` is naive, astimezone() will raise a ValueError,
    # so we don't need to perform a redundant check.
    value = value.astimezone(timezone)
    if hasattr(timezone, 'normalize'):
        # This method is available for pytz time zones.
        value = timezone.normalize(value)
    return value.replace(tzinfo=None)


def localtime(value: datetime, timezone: tzinfo = utc) -> datetime:
    """
    Converts an aware datetime.datetime to local time.

    Local time is utc by default, unless another time zone
    is specified.
    """
    # If `value` is naive, astimezone() will raise a ValueError,
    # so we don't need to perform a redundant check.
    value = value.astimezone(timezone)
    if hasattr(timezone, 'normalize'):
        # This method is available for pytz time zones.
        value = timezone.normalize(value)
    return value


def tz_now(truncate_second=False) -> datetime:
    """
    Returns an aware datetime.datetime.
    """
    # timeit shows that datetime.now(tz=utc) is 24% slower
    now = datetime.utcnow().replace(tzinfo=utc)
    if truncate_second:
        now = now.replace(microsecond=0)
    return now
