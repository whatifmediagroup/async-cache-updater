import os

REDIS_CACHE_URL = os.environ.get(
    'TEST_REDIS_CACHE_URL', 'redis://localhost:6379/10')
