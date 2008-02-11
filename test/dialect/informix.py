import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy.databases import informix
from testlib import *


class BasicTest(TestBase, AssertsExecutionResults):
    # A simple import of the database/ module should work on all systems.
    def test_import(self):
        # we got this far, right?
        return True


if __name__ == "__main__":
    testenv.main()
