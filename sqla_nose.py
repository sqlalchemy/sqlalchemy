#!/usr/bin/env python
"""
nose runner script.

This script is a front-end to "nosetests" which doesn't
require that SQLA's testing plugin be installed via setuptools.

"""
import sys

try:
    from sqlalchemy_nose.noseplugin import NoseSQLAlchemy
except ImportError:
    from os import path
    sys.path.append(path.join(path.dirname(path.abspath(__file__)), 'lib'))
    from sqlalchemy_nose.noseplugin import NoseSQLAlchemy

import nose

if __name__ == '__main__':
    nose.main(addplugins=[NoseSQLAlchemy()])
