#!/usr/bin/env python
"""
nose runner script.

Only use this script if setuptools is not available, i.e. such as
on Python 3K.  Otherwise consult README.unittests for the 
recommended methods of running tests.

"""
try:
    import sqlalchemy
except ImportError:
    from os import path
    import sys
    sys.path.append(path.join(path.dirname(__file__), 'lib'))

import nose
from sqlalchemy.test.noseplugin import NoseSQLAlchemy
from sqlalchemy.util import py3k


if __name__ == '__main__':
    if py3k:
        # this version breaks verbose output,
        # but is the only API that nose3 currently supports
        nose.main(plugins=[NoseSQLAlchemy()])
    else:
        # this is the "correct" API
        nose.main(addplugins=[NoseSQLAlchemy()])
