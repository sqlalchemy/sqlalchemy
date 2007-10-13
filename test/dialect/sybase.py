import testbase
from sqlalchemy import *
from sqlalchemy.databases import sybase
from testlib import *

class BasicTest(AssertMixin):
    # A simple import of the database/ module should work on all systems.
    def test_import(self):
        # we got this far, right?
        return True


if __name__ == "__main__":
    testbase.main()
