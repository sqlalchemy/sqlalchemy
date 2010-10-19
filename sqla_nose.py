#!/usr/bin/env python
"""
nose runner script.

Only use this script if setuptools is not available, i.e. such as
on Python 3K.  Otherwise consult README.unittests for the
recommended methods of running tests.

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
    py3k = getattr(sys, 'py3kwarning', False) or sys.version_info >= (3, 0)
    if py3k:
        # this version breaks verbose output,
        # but is the only API that nose3 currently supports
        nose.main(plugins=[NoseSQLAlchemy()])
    else:
        # this is the "correct" API
        nose.main(addplugins=[NoseSQLAlchemy()])
