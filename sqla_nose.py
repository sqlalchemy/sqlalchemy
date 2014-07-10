#!/usr/bin/env python
"""
nose runner script.

This script is a front-end to "nosetests" which
installs SQLAlchemy's testing plugin into the local environment.

"""
import sys
import nose
import warnings


from os import path
for pth in ['./lib']:
    sys.path.insert(0, path.join(path.dirname(path.abspath(__file__)), pth))

# installing without importing SQLAlchemy, so that coverage includes
# SQLAlchemy itself.
path = "lib/sqlalchemy/testing/plugin/noseplugin.py"
if sys.version_info >= (3, 3):
    from importlib import machinery
    noseplugin = machinery.SourceFileLoader("noseplugin", path).load_module()
else:
    import imp
    noseplugin = imp.load_source("noseplugin", path)


nose.main(addplugins=[noseplugin.NoseSQLAlchemy()])
