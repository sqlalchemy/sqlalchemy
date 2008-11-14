"""First import for all test cases, sets sys.path and loads configuration."""

import sys, os, logging, warnings

if sys.version_info < (2, 4):
    warnings.filterwarnings('ignore', category=FutureWarning)


from testlib.testing import main
import testlib.config


_setup = False

def configure_for_tests():
    """import testenv; testenv.configure_for_tests()"""

    global _setup
    if not _setup:
        sys.path.insert(0, os.path.join(os.getcwd(), 'lib'))
        logging.basicConfig()

        testlib.config.configure()
        _setup = True

def simple_setup():
    """import testenv; testenv.simple_setup()"""

    global _setup
    if not _setup:
        sys.path.insert(0, os.path.join(os.getcwd(), 'lib'))
        logging.basicConfig()

        testlib.config.configure_defaults()
        _setup = True

