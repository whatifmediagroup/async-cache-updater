"""Caches the output of functions with time-based buckets"""

__author__ = """RevPoint Media"""
__email__ = 'tech@jangl.com'
__version__ = '0.1.1'

__all__ = ['async_cache_updater', 'cache_settings', 'setup_client']


from async_cache_updater.settings import cache_settings, setup_client
from async_cache_updater.decorators import async_cache_updater
