#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['redis', 'asgiref', 'hiredis', 'python-dateutil', 'pytz', ]

test_requirements = ['pytest>=3', ]

setup(
    author="RevPoint Media",
    author_email='tech@jangl.com',
    python_requires='>=3.9',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
    ],
    description="Caches the output of functions with time-based buckets",
    entry_points={
        'console_scripts': [
            'async_cache_updater=async_cache_updater.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='async_cache_updater',
    name='async-cache-updater',
    packages=find_packages(include=['async_cache_updater', 'async_cache_updater.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/revpoint/async-cache-updater',
    version='1.0.0',
    zip_safe=False,
)
