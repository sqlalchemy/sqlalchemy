#!/usr/bin/env python
"""
pytest plugin script.

This script is an extension to py.test which
installs SQLAlchemy's testing plugin into the local environment.

"""
import sys

from os import path
for pth in ['../lib']:
    sys.path.insert(0, path.join(path.dirname(path.abspath(__file__)), pth))

from sqlalchemy.testing.plugin.pytestplugin import *
