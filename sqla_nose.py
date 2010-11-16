#!/usr/bin/env python
"""
nose runner script.

This script is a front-end to "nosetests" which
installs SQLAlchemy's testing plugin into the local environment.

"""
import sys

from os import path
for pth in ['.', './lib']:
    sys.path.insert(0, path.join(path.dirname(path.abspath(__file__)), pth))

from test.bootstrap.noseplugin import NoseSQLAlchemy

import nose

if __name__ == '__main__':
    nose.main(addplugins=[NoseSQLAlchemy()])
