#!/usr/bin/env python
"""
nose runner script.

This script is a front-end to "nosetests" which doesn't
require that SQLA's testing plugin be installed via setuptools.

"""
import sys

from os import path
for pth in ['.', './lib']:
    sys.path.insert(0, path.join(path.dirname(path.abspath(__file__)), pth))

from test.bootstrap.noseplugin import NoseSQLAlchemy

import nose

if __name__ == '__main__':
    nose.main(addplugins=[NoseSQLAlchemy()])
