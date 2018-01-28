#!/usr/bin/env python
"""
    Weather Monitoring Daemon.
"""

try:
    import setuptools
except ImportError:
    print("=========================================================")
    print(" Weather Daemon requires setuptools for installing         ")
    print(" You can install setuptools using pip:                   ")
    print("    $ pip install setuptools                             ")
    print("=========================================================")
    exit(1)


from setuptools import setup
import relay_daemon

setup(
    name         = 'Weather Daemon', # This is the name of your PyPI-package.
    version      = __version__,
    url          = __url__,
    author       = __author__,
    author_email = __email__,
    entry_points ={
        "console_scripts": ["weather_daemon = weather_daemon.main:main"]
    }
)
