import datetime
from typing import Any, Awaitable, Callable, Optional, Union

import pytz


CachedFunctionType = Callable[..., Awaitable[Any]]
CachedDecoratorType = Union[
    CachedFunctionType,
    Callable[[CachedFunctionType], CachedFunctionType],
]

BucketMethodType = Callable[[datetime.datetime, pytz.timezone], str]
BucketTypes = Optional[Union[str, BucketMethodType]]
DefaultDTMethodType = Callable[[], datetime.datetime]
TZLookupMethodType = Callable[..., Union[str, Awaitable[str]]]
