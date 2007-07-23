"""First import for all test cases, sets sys.path and loads configuration."""

__all__ = 'db',

import sys, os, logging
sys.path.insert(0, os.path.join(os.getcwd(), 'lib'))
logging.basicConfig()

import testlib.config
testlib.config.configure()

from testlib.testing import main
db = testlib.config.db

