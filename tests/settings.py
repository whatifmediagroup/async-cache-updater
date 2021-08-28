import os

REDIS_CACHE_URL = os.environ.get('REDIS_CACHE_URL', 'redis://localhost:6379/10')
