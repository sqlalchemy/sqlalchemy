#!/usr/bin/env python
"""
nose runner script.

This script is a front-end to "nosetests" which
installs SQLAlchemy's testing plugin into the local environment.

"""
import sys
import imp
import nose


from os import path
for pth in ['./lib']:
    sys.path.insert(0, path.join(path.dirname(path.abspath(__file__)), pth))

# installing without importing SQLAlchemy, so that coverage includes
# SQLAlchemy itself.
path = "lib/sqlalchemy/testing/plugin/noseplugin.py"
noseplugin = imp.load_source("noseplugin", path)


nose.main(addplugins=[noseplugin.NoseSQLAlchemy()])
